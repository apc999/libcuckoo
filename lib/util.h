#ifndef UTIL_H
#define UTIL_H

#include "cuckoohash_config.h"  // KeyType

#define mutex_lock(mutex) while (pthread_mutex_trylock(mutex));

#define mutex_unlock(mutex) pthread_mutex_unlock(mutex)

static inline
size_t cheap_rand() {
    static size_t cnt = 0;
    static size_t r1, r2;
    if ((cnt & 0xff) == 0) {
        r2 = rand();
    }
    if ((cnt & 0xff) == 0x80) {
        r1 = rand();
    }
    r1 += r2;
    cnt ++;
    return r1;
}

//#define keycmp(p1, p2) (memcmp(p1, p2, sizeof(KeyType)) == 0)

#define INT_KEYCMP_UNIT uint32_t

static uint64_t keycmp_mask[] = {0x0000000000000000ULL,
                                 0x00000000000000ffULL,
                                 0x000000000000ffffULL,
                                 0x0000000000ffffffULL,
                                 0x00000000ffffffffULL,
                                 0x000000ffffffffffULL,
                                 0x0000ffffffffffffULL,
                                 0x00ffffffffffffffULL};
static inline
bool keycmp(const char* key1, const char* key2) {

    INT_KEYCMP_UNIT v_key1;
    INT_KEYCMP_UNIT v_key2;
    size_t len = sizeof(KeyType);
    size_t k = 0;
    while ((len ) >= k + sizeof(INT_KEYCMP_UNIT)) {
        v_key1 = *(INT_KEYCMP_UNIT *) (key1 + k);
        v_key2 = *(INT_KEYCMP_UNIT *) (key2 + k);
        if (v_key1 != v_key2)
            return false;
        k += sizeof(INT_KEYCMP_UNIT);
    }
    /*
     * this code only works for little endian
     */
    if (len - k) {
        v_key1 = *(INT_KEYCMP_UNIT *) (key1 + k);
        v_key2 = *(INT_KEYCMP_UNIT *) (key2 + k);
        return (((v_key1 ^ v_key2) & keycmp_mask[len - k]) == 0);
    }
    return true;
}

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"


#if DEBUG
#  define DBG(fmt, args...)  fprintf(stderr, ANSI_COLOR_GREEN"[libcuckoo:%s:%d] "fmt""ANSI_COLOR_RESET,__FILE__,__LINE__,args)
#else
#  define DBG(fmt, args...)  do {} while (0)
#endif


#endif
