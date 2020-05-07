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

import unittest, teafiles, os, tables

type Tick = object
  date: int64
  price: float64
  volume: int64

suite "reading files":
  test "reading a file while knowing the type":
    var f = teafile[Tick]("acme.tea")
    echo f
    f.close()

  test "reading an untyped file":
    var f = dynteafile("acme.tea")
    echo f
    echo "We can read fields dynamically, knowing the type: ", f[0]["Time", int64]
    echo "...or even not knowing it: ", f[0]["Time"]
    echo "We can also iterate over a single field:"
    for price in f.col("Price", float64):
      echo price
    echo "Even without knowing the exact type:"
    for price in f.floatcol("Price"):
      echo price
    f.close()

suite "writing files":
  test "writing a file and reading it back":
    var f = teafile[Tick]("acme.tea")
    var w = create("acme2.tea", meta(f))
    for tick in f:
      append[Tick](w, tick)
    w.close()
    f.close()
    var g = dynteafile("acme2.tea")
    echo g
    g.close()
    removeFile("acme2.tea")

suite "metadata":
  test "reading metadata from a file":
    var f = teafile[Tick]("acme.tea")
    let metadata = meta(f)
    check metadata.content.description == "ACME at NYSE"
    check metadata.namevalues.ints["decimals"] == 2
    check metadata.time.ticksPerDay == 86400000
    check metadata.items.fields[0].name == "Time"
    f.close()