noinst_PROGRAMS = test_cuckoo test_cuckoo_mt bench_cuckoo bench_cuckoo_w_mr bench_cuckoo_rw insert_throughput

# change glibc_dir to the folder where glibc with TSX lock elision is installed
#glibc_dir = /home/xiaozhou/Research/build/glibc

glibc_ld = 
# comment out the following line if glibc with TSX lock elision is NOT installed
#glibc_ld = $(glibc_dir)/nptl/libpthread.a $(glibc_dir)/nptl/libpthread_nonshared.a $(glibc_dir)/libc.a

ldFlags = -all-static -L../lib -L$(top_srcdir)/lib -lcuckoo -lc -lpthread

test_cuckoo_SOURCES = test_cuckoo.cc
test_cuckoo_LDFLAGS = $(ldFlags)
test_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
test_cuckoo_LDADD = $(glibc_ld)

test_cuckoo_mt_SOURCES = test_cuckoo_mt.cc
test_cuckoo_mt_LDFLAGS = $(ldFlags)
test_cuckoo_mt_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
test_cuckoo_mt_LDADD = $(glibc_ld)

bench_cuckoo_SOURCES = bench_cuckoo.cc
bench_cuckoo_LDFLAGS = $(ldFlags)
bench_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
bench_cuckoo_LDADD = $(glibc_ld)

bench_cuckoo_w_mr_SOURCES = bench_cuckoo_w_mr.cc
bench_cuckoo_w_mr_LDFLAGS = $(ldFlags)
bench_cuckoo_w_mr_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
bench_cuckoo_w_mr_LDADD = $(glibc_ld)

bench_cuckoo_rw_SOURCES = bench_cuckoo_rw.cc
bench_cuckoo_rw_LDFLAGS = $(ldFlags)
bench_cuckoo_rw_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
bench_cuckoo_rw_LDADD = $(glibc_ld)

insert_throughput_SOURCES = insert_throughput.cc
insert_throughput_LDFLAGS = $(ldFlags)
insert_throughput_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la
insert_throughput_LDADD = $(glibc_ld)


AM_CFLAGS = -Wall -pthread
AM_CPPFLAGS = -std=c++0x -I../lib -I$(top_srcdir)/lib
