libcuckoo-sourcery
==================

libcuckoo provides a high-performance, compact hash table that allows
multiple concurrent reader and writer threads.

## This repository is purely for reference purposes.  It is a direct import of libcuckoo-dev from sourcery/indefero, primarily to make the code from our EuroSys 2014 paper available (e.g., the bfs branches).  Please *do not* commit any changes to this repository.

For the up-to-date C++ implementation of cuckoo hashing, look at the [libcuckoo](https://github.com/efficient/libcuckoo) repository.


Licence
-------
Copyright (C) 2013, Carnegie Mellon University and Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

---------------------------

CityHash (lib/city.h, lib/city.cc) is Copyright (c) Google, Inc. and
has its own license, as detailed in the source files.
