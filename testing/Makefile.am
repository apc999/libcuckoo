testLDFLAGS = -lpthread -L$(top_srcdir)/lib -lcityhash
testDEPS = $(top_srcdir)/lib/libcityhash.a

noinst_PROGRAMS = test_insert_and_find.out test_iterator.out stress_all.out insert_throughput.out

test_insert_and_find_out_SOURCES = test_insert_and_find.cc
test_insert_and_find_out_LDFLAGS = $(testLDFLAGS)
test_insert_and_find_out_DEPENDENCIES = $(testDEPS)

test_iterator_out_SOURCES = test_iterator.cc
test_iterator_out_LDFLAGS = $(testLDFLAGS)
test_iterator_out_DEPENDENCIES = $(testDEPS)

stress_all_out_SOURCES = stress_all.cc
stress_all_out_LDFLAGS = $(testLDFLAGS)
stress_all_out_DEPENDENCIES = $(testDEPS)

insert_throughput_out_SOURCES = insert_throughput.cc
insert_throughput_out_LDFLAGS = $(testLDFLAGS)
insert_throughput_out_DEPENDENCIES = $(testDEPS)

AM_CPPFLAGS = -I$(top_srcdir)/lib
