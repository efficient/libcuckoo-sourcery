/* Shared RTM header. Emulate TSX intrinsics for compilers and assemblers
   that do not support the intrinsics and instructions yet. */
#include "pthread.h"

#ifndef _HLE_H
#define _HLE_H 1

#ifdef __ASSEMBLER__

.macro XBEGIN target
	.byte 0xc7,0xf8
	.long \target-1f
1:
.endm

.macro XEND
	.byte 0x0f,0x01,0xd5
.endm

.macro XABORT code
	.byte 0xc6,0xf8,\code
.endm

.macro XTEST
	 .byte 0x0f,0x01,0xd6
.endm

#endif

/* Official RTM intrinsics interface matching gcc/icc, but works
   on older gcc compatible compilers and binutils.
   We should somehow detect if the compiler supports it, because
   it may be able to generate slightly better code. */

#define _XBEGIN_STARTED		(~0u)
#define _ABORT_EXPLICIT	        (1 << 0)
#define _ABORT_RETRY		(1 << 1)
#define _ABORT_CONFLICT	        (1 << 2)
#define _ABORT_CAPACITY	        (1 << 3) 
#define _ABORT_DEBUG		(1 << 4)
#define _ABORT_NESTED		(1 << 5)
#define _XABORT_CODE(x)		(((x) >> 24) & 0xff)

#define _ABORT_LOCK_BUSY 	0xff

#define _MAX_TRY_XBEGIN         10
#define _MAX_ABORT_RETRY        5


#ifndef __ASSEMBLER__

#define __force_inline __attribute__((__always_inline__)) inline

static __force_inline int _xbegin(void)
{
  int ret = _XBEGIN_STARTED;
  __asm__ volatile (".byte 0xc7,0xf8 ; .long 0" : "+a" (ret) :: "memory");
  return ret;
}

static __force_inline void _xend(void)
{
  __asm__ volatile (".byte 0x0f,0x01,0xd5" ::: "memory");
}

static __force_inline void _xabort(const unsigned int status)
{
  __asm__ volatile (".byte 0xc6,0xf8,%P0" :: "i" (status) : "memory");
}

static __force_inline int _xtest(void)
{
  unsigned char out;
  __asm__ volatile (".byte 0x0f,0x01,0xd6 ; setnz %0" : "=r" (out) :: "memory");
  return out;
}

static __force_inline int _lock_elision (pthread_mutex_t *mutex)
{
  unsigned status;
  int abort_retry = 0;

  for(int try_xbegin = 0; try_xbegin < _MAX_TRY_XBEGIN; try_xbegin ++) {
    if ((status = _xbegin()) == _XBEGIN_STARTED) {

      /* POSIX pthreads locking does not export an operation to query
	 a lock's state directly. So we have to query the internal
	 attribute of pthread_mutex_t data structure. */
      if ((mutex)->__data.__lock == 0)
	return 0;

      /* Lock was busy. Fall back to normal locking. */
      _xabort (_ABORT_LOCK_BUSY);
    }

    if (!(status & _ABORT_RETRY)) {
      if (abort_retry >= _MAX_ABORT_RETRY)
	break;
      abort_retry ++;
    }
  }

  /* Use a normal lock here.  */
  return pthread_mutex_lock(mutex);
}

static __force_inline int _unlock_elision(pthread_mutex_t *mutex) {

  if ((mutex)->__data.__lock == 0) {
    _xend();
    return 0;
  }

  return pthread_mutex_unlock(mutex);
}


#endif
#endif
