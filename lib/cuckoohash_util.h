#ifndef _CUCKOOHASH_UTIL_H
#define _CUCKOOHASH_UTIL_H

#include <cstdint>

/*
 * The array of version counter
 */
#define  counter_size  ((uint32_t)1 << (13))
#define  counter_mask  (counter_size - 1)


#define reorder_barrier() __asm__ __volatile__("" ::: "memory")
#define likely(x)     __builtin_expect((x), 1)
#define unlikely(x)   __builtin_expect((x), 0)

#define hashsize(n) ((uint32_t) 1 << n)
#define hashmask(n) (hashsize(n) - 1)

#endif
