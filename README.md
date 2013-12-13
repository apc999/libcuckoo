libcuckoo
=========

High-performance Concurrent Cuckoo Hashing Library

This library provides a compact hash table that allows multiple
concurrent reader and writer threads.

Authors: Bin Fan, David G. Andersen, Manu Goyal, and Michael Kaminsky

For details about this algorithm and citations, please refer to [our paper in NSDI 2013][1].

   [1]: http://www.cs.cmu.edu/~dga/papers/memc3-nsdi20013.pdf "MemC3: Compact and Concurrent Memcache with Dumber Caching and Smarter Hashing"

Requirements
================

This library has been tested on Mac OSX >= 10.8 and Ubuntu >= 12.04.

It compiles with clang++ >= 3.1 and g++ >= 4.6, however we highly
recommend the latest versions of both compilers, as they have greatly
improved support for atomic operations. Building the library requires
the autotools.

Building
==========

    $ autoreconf -fis
    $ ./configure
    $ make

Usage
==========

To build a program with the hash table, include ``cuckoohash_map.hh``
and optionally ``cuckoohash_config.h`` into your source file. Then
compile with the flags ``-I[libcuckoo_dir]/lib -pthread`` and link
with the flags ``-lpthread -L[libcuckoo_dir]/lib -lcityhash``. You
must also enable C++11 features on your compiler. If compiling a file
``test.cpp`` with g++, it might look like this:

    $ g++ -Ilibcuckoo/lib -pthread -std=c++11 -Llibcuckoo/lib -lcityhash test.cpp

With clang++ it might look like this:

    $ clang++ -Ilibcuckoo/lib -pthread -std=c++11 -stdlib=libc++ -Llibcuckoo/lib -lcityhash test.cpp

Tests
==========

The testing directory contains a number of tests and benchmarks of the
hash table, which also serve as useful examples of how to use the
table's various features. The entire test suite can be run with the
``runtest.py`` script, and individual tests, which have the suffix
".out" can be run individually as well.

Documentation
==================

The Doxygen-generated documentation is available at the
[project page](http://manugoyal.github.io/libcuckoo/).

Issue Report
============

To let us know your questions or issues, we recommend you
[report an issue](https://github.com/manugoyal/libcuckoo/issues) on
github. You can also email us at
[libcuckoo-dev@googlegroups.com](mailto:libcuckoo-dev@googlegroups.com).
