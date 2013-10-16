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

    /*
      Atomic operations on the version counters
    // read the counter, ensured by x86 memory ordering model
    inline void start_read_counter2(const TableInfo* ti, const size_t i1, const size_t i2, uint32_t v1, uint32_t v2) {
        do {
            v1 = *(volatile uint32_t *)&(ti->counters_[i1 & counter_mask]);
            v2 = *(volatile uint32_t *)&(ti->counters_[i2 & counter_mask]);
            reorder_barrier();
        } while(0);
    }
    inline void end_read_counter2(const TableInfo* ti, const size_t i1, const size_t i2, uint32_t v1, uint32_t v2) {
        do {
            reorder_barrier();
            v1 = *(volatile uint32_t *)&(ti->counters_[i1 & counter_mask]);
            v2 = *(volatile uint32_t *)&(ti->counters_[i2 & counter_mask]);
        } while(0);
    }

    // Atomic increase the counter
    inline void start_incr_counter(const TableInfo* ti, const size_t idx) {
        do {
            ((volatile uint32_t *)ti->counters_.get())[idx & counter_mask]++;
            reorder_barrier();
        } while(0);
    }
    inline void end_incr_counter(const TableInfo* ti, const size_t idx) {
        do {
            reorder_barrier();
            ((volatile uint32_t *)ti->counters_.get())[idx & counter_mask]++;
        } while(0);
    }
    inline void start_incr_counter2(const TableInfo* ti, const size_t i1, const size_t i2) {
        do {
            if (likely((i1 & counter_mask) != (i2 & counter_mask))) {
                ((volatile uint32_t *)ti->counters_.get())[i1 & counter_mask]++;
                ((volatile uint32_t *)ti->counters_.get())[i2 & counter_mask]++;
            } else {
                ((volatile uint32_t *)ti->counters_.get())[i1 & counter_mask]++;
            }
            reorder_barrier();
        } while(0);
    }

    inline void end_incr_counter2(const TableInfo* ti, const size_t i1, const size_t i2) {
        do {
            reorder_barrier();
            if (likely((i1 & counter_mask) != (i2 & counter_mask))) {
                ((volatile uint32_t *)ti->counters_.get())[i1 & counter_mask]++;
                ((volatile uint32_t *)ti->counters_.get())[i2 & counter_mask]++;
            } else {
                ((volatile uint32_t *)ti->counters_.get())[i1 & counter_mask]++;
            }
        } while(0);
    }

    // dga does not think we need this mfence in end_incr, because
    // the current code will call pthread_mutex_unlock before returning
    // to the caller;  pthread_mutex_unlock is a memory barrier:
    // http://www.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap04.html#tag_04_11
    // __asm__ __volatile("mfence" ::: "memory");
    */


#endif
