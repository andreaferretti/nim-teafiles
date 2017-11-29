# Copyright 2016 UniCredit S.p.A.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os, memfiles, streams, tables

const teaForTwoAtFive = 0x0d0e0a0402080500

### TYPES ###

type
  Uuid = array[16, byte]
  Section = enum
    itemSection =      0x0a.int32
    timeSection =      0x40.int32
    contentSection =   0x80.int32
    nameValueSection = 0x81.int32
  FieldType* = enum
    DoesNotExist = 0 # just because it is used as discriminator
    Int8 =   1
    Int16 =  2
    Int32 =  3
    Int64 =  4
    UInt8 =  5
    UInt16 = 6
    UInt32 = 7
    UInt64 = 8
    Float =  9
    Double = 10
  ValueType = enum
    intType =    1
    floatType =  2
    stringType = 3
    uuidType =   4
  UncheckedArray{.unchecked.}[T] = array[1, T]
  Field = object
    tp: FieldType
    offset: int32
    name: string
  Header = object
    itemStart: int64
    itemEnd: int64
    sectionCount: int64
  ContentSection = object
    description: string
  TimeSection = object
    epoch: int64
    ticksPerDay: int64
    timeFields: seq[int32]
  NameValueSection = object
    ints: Table[string, int32]
    floats: Table[string, float64]
    strings: Table[string, string]
    uuids: Table[string, Uuid]
  ItemSection = object
    size: int32
    name: string
    fields: seq[Field]
  TeaFile*[T] = object
    header: Header
    content: ContentSection
    time: TimeSection
    namevalues: NameValueSection
    items: ItemSection
    data: ptr[UncheckedArray[T]]
    underlying: MemFile
  Dyn* = object
    layout: seq[Field]
    position: pointer
  DynTeaFile* = object
    header: Header
    content: ContentSection
    time: TimeSection
    namevalues: NameValueSection
    items: ItemSection
    data: ptr[UncheckedArray[byte]]
    underlying: MemFile
  AnyTeaFile = TeaFile or DynTeaFile
  Number = object
    case kind*: FieldType
    of DoesNotExist:
      discard
    of Int8:
      int8value*: int8
    of Int16:
      int16value*: int16
    of Int32:
      int32value*: int32
    of Int64:
      int64value*: int64
    of UInt8:
      uint8value*: uint8
    of UInt16:
      uint16value*: uint16
    of UInt32:
      uint32value*: uint32
    of UInt64:
      uint64value*: uint64
    of Float:
      float32value*: float32
    of Double:
      float64value*: float64
  Meta = object
    content: ContentSection
    time: TimeSection
    namevalues: NameValueSection
    items: ItemSection
  WritableTeaFile = object
    meta: Meta
    stream: Stream

### POINTER HELPERS ###

template advance(p: pointer, b: int) =
  p = cast[pointer](cast[int](p) + b)

template next(p: pointer, T: typedesc): auto =
  let x = cast[ptr T](p)[]
  when T is Section or T is ValueType or T is FieldType:
    advance(p, 4)
  else:
    advance(p, sizeof(T))
  x

template nextString(p: pointer): auto =
  # let
  #   length = p.next(int32)
  #   bytes = cast[ptr UncheckedArray[char]](p)
  let length = cast[ptr int32](p)[]
  advance(p, sizeof(int32))
  let bytes = cast[ptr UncheckedArray[char]](p)
  var s = newString(length)
  for i in 0'i64 ..< length:
    s[i.int] = bytes[i.int]
  advance(p, length.int)
  s

### READING PREAMBLE ###

proc readHeader(tea: var AnyTeaFile) =
  var cursor = tea.underlying.mem
  let magic = cursor.next(int64)
  assert magic == teaForTwoAtFive
  tea.header.itemStart = cursor.next(int64)
  tea.header.itemEnd = cursor.next(int64)
  tea.header.sectionCount = cursor.next(int64)
  assert tea.header.sectionCount >= 0 and tea.header.sectionCount <= 4
  assert tea.header.itemStart > 0
  assert tea.header.itemEnd >= 0

proc readContentSection(tea: var AnyTeaFile, cursor: pointer) =
  var c = cursor
  tea.content.description = c.nextString()

proc readTimeSection(tea: var AnyTeaFile, cursor: pointer) =
  var c = cursor
  tea.time.epoch = c.next(int64)
  tea.time.ticksPerDay = c.next(int64)
  let L = c.next(int32)
  tea.time.timeFields = newSeq[int32](L)
  for i in 0 ..< L:
    tea.time.timeFields[i] = c.next(int32)

proc readNameValueSection(tea: var AnyTeaFile, cursor: pointer) =
  tea.namevalues.ints = initTable[string, int32]()
  tea.namevalues.floats = initTable[string, float64]()
  tea.namevalues.strings = initTable[string, string]()
  tea.namevalues.uuids = initTable[string, Uuid]()
  var c = cursor
  let length = c.next(int32)
  for _ in 1 .. length:
    let key = c.nextString()
    case c.next(ValueType):
      of intType:
        tea.namevalues.ints.add(key, c.next(int32))
      of floatType:
        tea.namevalues.floats.add(key, c.next(float64))
      of stringType:
        tea.namevalues.strings.add(key, c.nextString())
      of uuidType:
        tea.namevalues.uuids.add(key, c.next(Uuid))

proc readItemSection(tea: var AnyTeaFile, cursor: pointer) =
  var c = cursor
  tea.items.size = c.next(int32)
  tea.items.name = c.nextString()
  let length = c.next(int32)
  # assert length >= 1
  tea.items.fields = newSeq[Field](length)
  for i in 0 ..< length:
    tea.items.fields[i] = Field(
      tp: c.next(FieldType),
      offset: c.next(int32),
      name: c.nextString()
    )

proc readSection(tea: var AnyTeaFile, cursor: var pointer) =
  let id = cursor.next(Section)
  let offset = cursor.next(int32)
  case id:
    of contentSection:
      readContentSection(tea, cursor)
    of timeSection:
      readTimeSection(tea, cursor)
    of nameValueSection:
      readNameValueSection(tea, cursor)
    of itemSection:
      readItemSection(tea, cursor)
  cursor.advance(offset.int)

proc readSections(tea: var AnyTeaFile) =
  var cursor = tea.underlying.mem
  cursor.advance(32)
  for _ in 1 .. tea.header.sectionCount:
    readSection(tea, cursor)

proc readStart[T](tea: var TeaFile[T]) =
  var cursor = tea.underlying.mem
  cursor.advance(tea.header.itemStart.int)
  tea.data = cast[ptr[UncheckedArray[T]]](cursor)

proc readStart(tea: var DynTeaFile) =
  var cursor = tea.underlying.mem
  cursor.advance(tea.header.itemStart.int)
  tea.data = cast[ptr[UncheckedArray[byte]]](cursor)

### WRITING PREAMBLE ###

proc len(s: ContentSection): int32 =
  (4 + s.description.len).int32

proc len(s: TimeSection): int32 =
  (8 + 8 + 4 + 4 * s.timeFields.len).int32

proc len(s: NameValueSection): int32 =
  result = 12'i32
  for k, v in s.ints:
    result += (8 + len(k)).int32
  for k, v in s.floats:
    result += (12 + len(k)).int32
  for k, v in s.strings:
    result += (8 + len(v) + len(k)).int32
  for k, v in s.uuids:
    result += (16 + len(k)).int32

proc len(s: ItemSection): int32 =
  result = (12 + s.name.len).int32
  for field in s.fields:
    result += (12 + field.name.len).int32

proc lengthAndPadding(meta: Meta): tuple[length: int64, padding: int] =
  let
    length = (64 + meta.content.len + meta.time.len +
      meta.namevalues.len + meta.items.len).int64
    rem = length mod 8
    padding = if rem == 0: 0 else: 8 - rem.int
  return (length, padding)

proc len(meta: Meta): int64 =
  let (length, padding) = lengthAndPadding(meta)
  return length + padding

proc writeString(s: var Stream, text: string) =
  s.write(text.len.int32)
  s.write(text)

proc writeHeader(s: var Stream, meta: Meta) =
  s.write(teaForTwoAtFive)
  s.write(meta.len) # itemStart
  s.write(0'i64) # itemEnd
  s.write(4'i64) # sectionCount

proc writeContent(s: var Stream, meta: Meta) =
  s.write(contentSection.int32)
  s.write(meta.content.len)
  s.writeString(meta.content.description)

proc writeTimeSection(s: var Stream, meta: Meta) =
  s.write(timeSection.int32)
  s.write(meta.time.len)
  s.write(meta.time.epoch)
  s.write(meta.time.ticksPerDay)
  s.write(meta.time.timeFields.len.int32)
  for t in meta.time.timeFields:
    s.write(t)

proc writeItemSection(s: var Stream, meta: Meta) =
  s.write(itemSection.int32)
  s.write(meta.items.len)
  s.write(meta.items.size.int32)
  s.writeString(meta.items.name)
  s.write(meta.items.fields.len.int32)
  for field in meta.items.fields:
    s.write(field.tp.int32)
    s.write(field.offset)
    s.writeString(field.name)

proc writeNameValueSection(s: var Stream, meta: Meta) =
  s.write(nameValueSection.int32)
  s.write(meta.namevalues.len)
  let length = len(meta.namevalues.ints) + len(meta.namevalues.floats) +
    len(meta.namevalues.strings) + len(meta.namevalues.uuids)
  s.write(length.int32)
  for k, v in meta.namevalues.ints:
    s.writeString(k)
    s.write(intType.int32)
    s.write(v)
  for k, v in meta.namevalues.floats:
    s.writeString(k)
    s.write(floatType.int32)
    s.write(v)
  for k, v in meta.namevalues.strings:
    s.writeString(k)
    s.write(stringType.int32)
    s.writeString(v)
  for k, v in meta.namevalues.uuids:
    s.writeString(k)
    s.write(uuidType.int32)
    s.write(v)

proc writePadding(s: var Stream, meta: Meta) =
  let (_, padding) = lengthAndPadding(meta)
  for _ in 1.. padding:
    s.write(0'i8)

proc create*(path: string, meta: Meta): WritableTeaFile =
  result.meta = meta
  result.stream = newFileStream(path, fmWrite)
  result.stream.writeHeader(meta)
  result.stream.writeItemSection(meta)
  result.stream.writeContent(meta)
  result.stream.writeNameValueSection(meta)
  result.stream.writeTimeSection(meta)
  result.stream.writePadding(meta)

### OPENING AND CLOSING TEAFILES ###

proc teafile*[T](path: string): TeaFile[T] =
  let f = memfiles.open(path, mode = fmReadWrite)
  result.underlying = f
  readHeader(result)
  readSections(result)
  readStart(result)

proc dynteafile*(path: string): DynTeaFile =
  let f = memfiles.open(path, mode = fmRead)
  result.underlying = f
  readHeader(result)
  readSections(result)
  readStart(result)

proc close*(tea: var AnyTeaFile) = close(tea.underlying)

proc close*(tea: var WritableTeaFile) = close(tea.stream)

### ACCESSORS ###

proc len*[T](tea: TeaFile[T]): int64 =
  if tea.header.itemEnd == 0:
    (tea.underlying.size - tea.header.itemStart) div sizeof(T)
  else:
    (tea.header.itemEnd - tea.header.itemStart) div sizeof(T)

proc len*(tea: DynTeaFile): int64 =
  if tea.header.itemEnd == 0:
    (tea.underlying.size - tea.header.itemStart) div tea.items.size
  else:
    (tea.header.itemEnd - tea.header.itemStart) div tea.items.size

proc `[]`*[T](tea: TeaFile[T], i: int): T =
  assert i >= 0 and i < len(tea)
  tea.data[i]

proc `[]`*(tea: DynTeaFile, i: int): Dyn =
  assert i >= 0 and i < len(tea)
  result.layout = tea.items.fields
  result.position = tea.data
  advance(result.position, i * tea.items.size)

proc `[]=`*[T](tea: var TeaFile[T], i: int, val: T) =
  assert i >= 0 and i < len(tea)
  tea.data[i] = val

proc append*[T](tea: var WritableTeaFile, val: T) =
  tea.stream.write(val)

### ITERATORS ###

iterator items*[T](tea: TeaFile[T]): T {.inline.} =
  for i in 0'i64 ..< len(tea):
    yield tea.data[i]

iterator items*(tea: DynTeaFile): Dyn {.inline.} =
  var p = cast[pointer](tea.data)
  for _ in 0'i64 ..< len(tea):
    yield Dyn(layout: tea.items.fields, position: cast[ptr UncheckedArray[byte]](p))
    advance(p, tea.items.size)

iterator pairs*[T](tea: TeaFile[T]): tuple[key: int, val: T] {.inline.} =
  for i in 0'i64 ..< len(tea):
    yield (i, tea.data[i])

iterator pairs*(tea: DynTeaFile): tuple[key: int64, val: Dyn] {.inline.} =
  var p = cast[pointer](tea.data)
  for i in 0'i64 ..< len(tea):
    yield (i, Dyn(layout: tea.items.fields, position: cast[ptr UncheckedArray[byte]](p)))
    advance(p, tea.items.size)

iterator col*(tea: AnyTeaFile, key: string, T: typedesc): T =
  var
    offset: int32
    tp: FieldType
    found = false
  for f in tea.items.fields:
    if f.name == key:
      offset = f.offset
      tp = f.tp
      found = true
      break
  if not found:
    assert false
  when T is int8: assert tp == Int8
  when T is int16: assert tp == Int16
  when T is int32: assert tp == Int32
  when T is int64: assert tp == Int64
  when T is uint8: assert tp == UInt8
  when T is uint16: assert tp == UInt16
  when T is uint32: assert tp == UInt32
  when T is uint64: assert tp == UInt64
  when T is float32: assert tp == Float
  when T is float64: assert tp == Double
  var p = cast[pointer](tea.data)
  advance(p, offset)
  for i in 0'i64 ..< len(tea):
    yield cast[ptr T](p)[]
    advance(p, tea.items.size)

iterator intcol*(tea: AnyTeaFile, key: string): int =
  var
    offset: int32
    tp: FieldType
    found = false
  for f in tea.items.fields:
    if f.name == key:
      offset = f.offset
      tp = f.tp
      found = true
      break
  if not found:
    assert false
  var p = cast[pointer](tea.data)
  advance(p, offset)
  for i in 0'i64 ..< len(tea):
    case tp:
      of DoesNotExist:
        assert false
      of Int8:
        yield int(cast[ptr int8](p)[])
      of Int16:
        yield int(cast[ptr int16](p)[])
      of Int32:
        yield int(cast[ptr int32](p)[])
      of Int64:
        yield int(cast[ptr int64](p)[])
      of UInt8:
        yield int(cast[ptr uint8](p)[])
      of UInt16:
        yield int(cast[ptr uint16](p)[])
      of UInt32:
        yield int(cast[ptr uint32](p)[])
      of UInt64:
        yield int(cast[ptr uint64](p)[])
      of Float:
        yield int(cast[ptr float32](p)[])
      of Double:
        yield int(cast[ptr float64](p)[])
    advance(p, tea.items.size)

iterator floatcol*(tea: AnyTeaFile, key: string): float =
  var
    offset: int32
    tp: FieldType
    found = false
  for f in tea.items.fields:
    if f.name == key:
      offset = f.offset
      tp = f.tp
      found = true
      break
  if not found:
    assert false
  var p = cast[pointer](tea.data)
  advance(p, offset)
  for i in 0'i64 ..< len(tea):
    case tp:
      of DoesNotExist:
        assert false
      of Int8:
        yield float(cast[ptr int8](p)[])
      of Int16:
        yield float(cast[ptr int16](p)[])
      of Int32:
        yield float(cast[ptr int32](p)[])
      of Int64:
        yield float(cast[ptr int64](p)[])
      of UInt8:
        yield float(cast[ptr uint8](p)[])
      of UInt16:
        yield float(cast[ptr uint16](p)[])
      of UInt32:
        yield float(cast[ptr uint32](p)[])
      of UInt64:
        yield float(cast[ptr uint64](p)[])
      of Float:
        yield float(cast[ptr float32](p)[])
      of Double:
        yield float(cast[ptr float64](p)[])
    advance(p, tea.items.size)

### ACCESS TO DYNAMIC FIELDS ###

proc `[]`*(d: Dyn, key: string): Number =
  var
    offset: int32
    tp: FieldType
    found = false
  for f in d.layout:
    if f.name == key:
      offset = f.offset
      tp = f.tp
      found = true
      break
  if not found:
    assert false
  var p = d.position
  advance(p, offset)
  case tp:
    of DoesNotExist:
      assert false
    of Int8:
      result = Number(kind: Int8, int8value: cast[ptr int8](p)[])
    of Int16:
      result = Number(kind: Int16, int16value: cast[ptr int16](p)[])
    of Int32:
      result = Number(kind: Int32, int32value: cast[ptr int32](p)[])
    of Int64:
      result = Number(kind: Int64, int64value: cast[ptr int64](p)[])
    of UInt8:
      result = Number(kind: UInt8, uint8value: cast[ptr uint8](p)[])
    of UInt16:
      result = Number(kind: UInt16, uint16value: cast[ptr uint16](p)[])
    of UInt32:
      result = Number(kind: UInt32, uint32value: cast[ptr uint32](p)[])
    of UInt64:
      result = Number(kind: UInt64, uint64value: cast[ptr uint64](p)[])
    of Float:
      result = Number(kind: Float, float32value: cast[ptr float32](p)[])
    of Double:
      result = Number(kind: Double, float64value: cast[ptr float64](p)[])

proc `[]`*(d: Dyn, key: string, T: typedesc): T =
  var
    offset: int32
    tp: FieldType
    found = false
  for f in d.layout:
    if f.name == key:
      offset = f.offset
      tp = f.tp
      found = true
      break
  if not found:
    assert false
  var p = d.position
  advance(p, offset)
  when T is int8: assert tp == Int8
  when T is int16: assert tp == Int16
  when T is int32: assert tp == Int32
  when T is int64: assert tp == Int64
  when T is uint8: assert tp == UInt8
  when T is uint16: assert tp == UInt16
  when T is uint32: assert tp == UInt32
  when T is uint64: assert tp == UInt64
  when T is float32: assert tp == Float
  when T is float64: assert tp == Double
  return cast[ptr T](p)[]

### PRINTING ###

proc `$`*(d: Dyn): string =
  result = ""
  for f in d.layout:
    result &= f.name & ": "
    var p = d.position
    advance(p, f.offset)
    case f.tp:
      of DoesNotExist:
        assert false
      of Int8:
        result &= $(cast[ptr int8](p)[]) & " "
      of Int16:
        result &= $(cast[ptr int16](p)[]) & " "
      of Int32:
        result &= $(cast[ptr int32](p)[]) & " "
      of Int64:
        result &= $(cast[ptr int64](p)[]) & " "
      of UInt8:
        result &= $(cast[ptr uint8](p)[]) & " "
      of UInt16:
        result &= $(cast[ptr uint16](p)[]) & " "
      of UInt32:
        result &= $(cast[ptr uint32](p)[]) & " "
      of UInt64:
        result &= $(cast[ptr uint64](p)[]) & " "
      of Float:
        result &= $(cast[ptr float32](p)[]) & " "
      of Double:
        result &= $(cast[ptr float64](p)[]) & " "

proc `$`(u: Uuid): string =
  result = newString(16)
  for i in 1 .. 16:
    result[i] = u[i].char

proc `$`(field: Field): string =
  field.name & " [" & $(field.tp) & "] -> " & $(field.offset)

proc printDescription*(tea: AnyTeaFile): string =
  result = ""
  if not tea.content.description.isNil:
    result &= "\nDescription:\n"
    result &= "------------\n"
    result &= tea.content.description & "\n"

proc printItems*(tea: AnyTeaFile): string =
  result = "\nItems:\n"
  result &= "------\n"
  result &= "Record name: " & tea.items.name & "\n"
  result &= "Record length: " & $(tea.items.size) & "\n"
  result &= "Field offsets:\n"
  for f in tea.items.fields:
    result &= "  " & $(f) & "\n"

proc printNameValue*(tea: AnyTeaFile): string =
  result = "\nName/Value pairs:\n"
  result &= "-----------------\n"
  for k, v in tea.namevalues.ints:
    result &= k & ": " & $(v) & "\n"
  for k, v in tea.namevalues.floats:
    result &= k & ": " & $(v) & "\n"
  for k, v in tea.namevalues.strings:
    result &= k & ": " & v & "\n"
  for k, v in tea.namevalues.uuids:
    result &= k & ": " & $(v) & "\n"

proc printTime*(tea: AnyTeaFile): string =
  result = "\nTime section:\n"
  result &= "-------------\n"
  result &= "Epoch: " & $(tea.time.epoch) & "\n"
  result &= "Ticks/day: " & $(tea.time.ticksPerDay) & "\n"
  result &= "Time fields: " & $(tea.time.timeFields) & "\n"

proc printExcerpt*(tea: AnyTeaFile, maxItems: int = 5): string =
  let length = len(tea)
  result = "\nLength: " & $(length) & "\n"
  result &= "\nExcerpt:\n"
  result &= "--------\n"
  for i in 0'i64 ..< min(maxItems, length):
    result &= $(tea[i.int]) & "\n"
  if length > maxItems:
    result &= "...\n"

proc print(tea: AnyTeaFile): string =
  result = printDescription(tea)
  result &= printItems(tea)
  result &= printNameValue(tea)
  result &= printTime(tea)
  result &= printExcerpt(tea)

proc `$`*[T](tea: TeaFile[T]): string = print(tea)

proc `$`*(tea: DynTeaFile): string = print(tea)

### CREATE METAS ###

proc meta*(t: AnyTeaFile): Meta =
  result.content = t.content
  result.time = t.time
  result.namevalues = t.namevalues
  result.items = t.items

proc meta*(description: string): Meta =
  result.content.description = description
  result.namevalues.ints = initTable[string, int32]()
  result.namevalues.floats = initTable[string, float64]()
  result.namevalues.strings = initTable[string, string]()
  result.namevalues.uuids = initTable[string, Uuid]()

proc `[]=`*(meta: var Meta, key: string, val: int32) =
  meta.namevalues.ints[key] = val

proc `[]=`*(meta: var Meta, key: string, val: float64) =
  meta.namevalues.floats[key] = val

proc `[]=`*(meta: var Meta, key: string, val: string) =
  meta.namevalues.strings[key] = val

proc `[]=`*(meta: var Meta, key: string, val: Uuid) =
  meta.namevalues.uuids[key] = val

### EXPORT TABLE FUNCTIONS NEEDED BY CLIENTS ###

export tables.pairs