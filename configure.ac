#           -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([libcuckoo], [1.0], [fawn-dev@mailman.srv.cs.cmu.edu])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_HEADERS([config.h])

AC_CONFIG_MACRO_DIR([m4])

CPPFLAGS=""
CFLAGS=""
CXXFLAGS=""
dnl Regular debugging
dnl AX_CHECK_COMPILE_FLAG([-g], [CPPFLAGS="$CPPFLAGS -g"])
dnl AX_CHECK_COMPILE_FLAG([-gdwarf-2], [CPPFLAGS="$CPPFLAGS -gdwarf-2"])

dnl Profiling
dnl AX_CHECK_COMPILE_FLAG([-pg], [CPPFLAGS="$CPPFLAGS -pg"])

dnl Optimized
AX_CHECK_COMPILE_FLAG([-O3], [CPPFLAGS="$CPPFLAGS -O3"])
AX_CHECK_COMPILE_FLAG([-DNDEBUG], [CPPFLAGS="$CPPFLAGS -DNDEBUG"])

# Determine flags specific to C++
AC_LANG_PUSH([C++])
AX_CHECK_COMPILE_FLAG([-std=c++0x], [CXXFLAGS="$CXXFLAGS -std=c++0x"])
AX_CHECK_COMPILE_FLAG([-stdlib=libc++], [CXXFLAGS="$CXXFLAGS -stdlib=libc++"])
AX_CHECK_COMPILE_FLAG([-Wall], [CXXFLAGS="$CXXFLAGS -Wall"])

# Checks for TBB. If it's there, tests will include and link with
# Intel TBB
AC_CHECK_LIB([tbb], [TBB_runtime_interface_version], [has_tbb=yes], [])
AC_CHECK_HEADER([tbb/tbb.h], [has_tbb_h=yes], [])
use_tbb_val=0
AS_IF([test "$has_tbb" = yes],
            AS_IF([test "$has_tbb_h" = yes],
                        AC_SUBST([TBB_LIBS], -ltbb)
                        use_tbb_val=1,
                        []),
            [])
AC_DEFINE_UNQUOTED([USE_TBB], [$use_tbb_val], [Whether -ltbb and its header file were found])

# Checks for Google dense_hash. If it's there, tests will include it.
AC_CHECK_HEADER([sparsehash/dense_hash_map], [has_dense_hash=yes], [])
use_dense_hash_val=0
AS_IF([test "$has_dense_hash" = yes],
            [use_dense_hash_val=1],
            [])
AC_DEFINE_UNQUOTED([USE_DENSE_HASH], [$use_dense_hash_val], [Whether sparsehash/dense_hash_map.h was found])

AC_LANG_POP([C++])

# Checks for programs.
AC_PROG_CXX

#AC_DISABLE_SHARED
AC_PROG_LIBTOOL

# Checks for libraries.
#AC_CHECK_LIB([pthread], [pthread_mutex_init], [], [echo "pthreads not found.  Please install pthread library before proceeding"; exit -1])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_PROG_GCC_TRADITIONAL
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC

AC_CHECK_HEADERS([getopt.h])

dnl Check if we're a little-endian or a big-endian system, needed by hash code

AC_DEFUN([AC_C_ENDIAN],
[AC_CACHE_CHECK(for endianness, ac_cv_c_endian,
[
  AC_RUN_IFELSE(
    [AC_LANG_PROGRAM([], [dnl
        long val = 1;
        char *c = (char *) &val;
        exit(*c == 1);
    ])
  ],[
    ac_cv_c_endian=big
  ],[
    ac_cv_c_endian=little
  ])
])
if test $ac_cv_c_endian = big; then
  AC_DEFINE(ENDIAN_BIG, 1, [machine is bigendian])
fi
if test $ac_cv_c_endian = little; then
  AC_DEFINE(ENDIAN_LITTLE, 1, [machine is littleendian])
fi
])

AC_C_ENDIAN


dnl Check if we have stdbool

#AC_HEADER_STDBOOL
AH_BOTTOM([#if HAVE_STDBOOL_H
#include <stdbool.h>
#else
#define bool char
#define false 0
#define true 1
#endif ])


AC_CONFIG_FILES([ Makefile
                  libcuckoo/Makefile
                  tests/Makefile
                  examples/Makefile
                ])

AC_OUTPUT
