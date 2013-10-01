GTEST_DIR = $(top_srcdir)/gtest
testLDFLAGS = -lpthread libgtest_main.a -L$(top_srcdir)/lib -lcuckoo
testDEPS = $(top_srcdir)/lib/libcuckoo.la

noinst_LIBRARIES = libgtest.a libgtest_main.a
libgtest_a_SOURCES = $(GTEST_DIR)/src/gtest-all.cc
libgtest_main_a_SOURCES = $(GTEST_DIR)/src/gtest_main.cc $(GTEST_DIR)/src/gtest-all.cc

noinst_PROGRAMS = test_insert.out
test_insert_out_SOURCES = test_insert.cc
test_insert_out_LDFLAGS = $(testLDFLAGS)
test_insert_out_DEPENDENCIES = $(testDEPS)

AM_CFLAGS = -g -Wall -Wextra -pthread
AM_CPPFLAGS = -std=c++0x -I$(top_srcdir)/lib -isystem $(GTEST_DIR)/include -I$(GTEST_DIR)
