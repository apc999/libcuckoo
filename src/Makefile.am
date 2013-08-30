noinst_PROGRAMS = test_cuckoo test_cuckoo_mt bench_cuckoo app_cuckoo

test_cuckoo_SOURCES = test_cuckoo.cc
test_cuckoo_LDFLAGS = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo -lcityhash
test_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

test_cuckoo_mt_SOURCES = test_cuckoo_mt.cc
test_cuckoo_mt_LDFLAGS = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo -lcityhash
test_cuckoo_mt_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

bench_cuckoo_SOURCES = bench_cuckoo.cc
bench_cuckoo_LDFLAGS = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo -lcityhash
bench_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

app_cuckoo_SOURCES = app_cuckoo.cc
app_cuckoo_LDFLAGS = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo -lcityhash
app_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

AM_CFLAGS = -Wall
AM_CPPFLAGS = -static -std=c++0x -I../lib -I$(top_srcdir)/lib -pthread
