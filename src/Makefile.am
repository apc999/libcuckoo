noinst_PROGRAMS = test_cuckoo test_cuckoo_mt bench_cuckoo

test_cuckoo_SOURCES = test_cuckoo.cc
test_cuckoo_LDFLAGS = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo -lc
test_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
test_cuckoo_LDADD = /home/kaminsky/build/glibc-tsx/nptl/libpthread.a /home/kaminsky/build/glibc-tsx/nptl/libpthread_nonshared.a /home/kaminsky/build/glibc-tsx/libc.a

test_cuckoo_mt_SOURCES = test_cuckoo_mt.cc
test_cuckoo_mt_LDFLAGS = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo
test_cuckoo_mt_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
test_cuckoo_mt_LDADD = /home/kaminsky/build/glibc-tsx/nptl/libpthread.a /home/kaminsky/build/glibc-tsx/nptl/libpthread_nonshared.a /home/kaminsky/build/glibc-tsx/libc.a

bench_cuckoo_SOURCES = bench_cuckoo.cc
bench_cuckoo_LDFLAGS = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo
bench_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
bench_cuckoo_LDADD = /home/kaminsky/build/glibc-tsx/nptl/libpthread.a /home/kaminsky/build/glibc-tsx/nptl/libpthread_nonshared.a /home/kaminsky/build/glibc-tsx/libc.a

AM_CFLAGS = -Wall
AM_CPPFLAGS = -static -std=c++0x -I../lib -I$(top_srcdir)/lib

