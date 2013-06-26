noinst_PROGRAMS = test_cuckoo test_cuckoo_mt bench_cuckoo test_cuckoo_path bench_cuckoo_seg bench_cuckoo_mt bench_cuckoo_mt_rw bench_cuckoo_rw

test_cuckoo_SOURCES = test_cuckoo.cc
test_cuckoo_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
test_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

test_cuckoo_path_SOURCES = test_cuckoo_path.cc
test_cuckoo_path_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
test_cuckoo_path_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

test_cuckoo_mt_SOURCES = test_cuckoo_mt.cc
test_cuckoo_mt_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
test_cuckoo_mt_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

bench_cuckoo_SOURCES = bench_cuckoo.cc
bench_cuckoo_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
bench_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

bench_cuckoo_seg_SOURCES = bench_cuckoo_seg.cc
bench_cuckoo_seg_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
bench_cuckoo_seg_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

bench_cuckoo_mt_SOURCES = bench_cuckoo_mt.cc
bench_cuckoo_mt_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
bench_cuckoo_mt_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

bench_cuckoo_mt_rw_SOURCES = bench_cuckoo_mt_rw.cc
bench_cuckoo_mt_rw_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
bench_cuckoo_mt_rw_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

bench_cuckoo_rw_SOURCES = bench_cuckoo_rw.cc
bench_cuckoo_rw_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
bench_cuckoo_rw_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

AM_CFLAGS = -Wall
AM_CPPFLAGS = -std=c++0x -I../lib -I$(top_srcdir)/lib
