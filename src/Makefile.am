noinst_PROGRAMS = test_cuckoo test_cuckoo_mt bench_cuckoo app_cuckoo

test_cuckoo_SOURCES = test_cuckoo.cc
test_cuckoo_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcityhash
test_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcityhash.a

test_cuckoo_mt_SOURCES = test_cuckoo_mt.cc
test_cuckoo_mt_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcityhash
test_cuckoo_mt_DEPENDENCIES = $(top_srcdir)/lib/libcityhash.a

bench_cuckoo_SOURCES = bench_cuckoo.cc
bench_cuckoo_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcityhash
bench_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcityhash.a

app_cuckoo_SOURCES = app_cuckoo.cc
app_cuckoo_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcityhash
app_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcityhash.a

AM_CPPFLAGS = -I../lib -I$(top_srcdir)/lib
