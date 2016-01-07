Nim TeaFiles
============

This is a Nim library to read and write [TeaFiles](http://discretelogics.com/teafiles/).

TeaFiles provide fast read/write access to time series data from any software
package on any platform. Time Series are considered homogeneous collections of
items, ordered by their timestamp. Items are stored in raw binary format, such
that data can be memory mapped for fast read/write access. In order to ensure
correct data interpretation when data is exchanged between multiple applications,
TeaFiles optionally embedd a description of the data layout in the file header,
along with other optional description of the file's contents.

Documentation
-------------

There is not much in terms of documentation yet, but you can have a look at

* the [tests usage](https://github.com/unicredit/nim-teafiles/blob/master/test.nim)
* the [API docs](http://unicredit.github.io/nim-teafiles/api.html)
* the [original TeaFiles spec](http://discretelogics.com/resources/teafilespec/)

License
-------

Unlike the TeaFiles packages provided for other languages, this library is
provided under the [Apache2 license](http://www.apache.org/licenses/LICENSE-2.0).
This is possible because it is a clean room implementation entirely derived
from the [TeaFiles specification](http://discretelogics.com/resources/teafilespec/).

Credits
-------

The TeaFiles format was originally designed and developed by [DiscreteLogics](http://discretelogics.com).
TeaFiles is a copyright by DiscreteLogics.