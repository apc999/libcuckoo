/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   cuckoohash.c
 * @author Bin Fan <binfan@cs.cmu.edu>
 * @date   Mon Feb 25 22:17:04 2013
 *
 * @brief  implementation of single-writer/multi-reader cuckoo hash
 *
 *
 */

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "config.h"
#include "city.h"
#include "util.h"

#include "cuckoohash.h"
#include "cuckoohash_config.h"


/*
 * The array of version counter
 */
#define  counter_size  ((uint32_t)1 << (13))
#define  counter_mask  (counter_size - 1)


#define reorder_barrier() __asm__ __volatile__("" ::: "memory")
#define likely(x)     __builtin_expect((x), 1)
#define unlikely(x)   __builtin_expect((x), 0)

/**
 *  @brief read the counter, ensured by x86 memory ordering model
 *
 */
#define start_read_counter(h, idx, version)                             \
    do {                                                                \
        version = *(volatile uint32_t *)(&((uint32_t*) h->counters)[idx & counter_mask]); \
        reorder_barrier();                                              \
    } while(0)

#define end_read_counter(h, idx, version)                               \
    do {                                                                \
        reorder_barrier();                                              \
        version = *(volatile uint32_t *)(&((uint32_t*) h->counters)[idx & counter_mask]); \
    } while (0)


#define start_read_counter2(h, i1, i2, v1, v2)                          \
    do {                                                                \
        v1 = *(volatile uint32_t *)(&((uint32_t*) h->counters)[i1 & counter_mask]); \
        v2 = *(volatile uint32_t *)(&((uint32_t*) h->counters)[i2 & counter_mask]); \
        reorder_barrier();                                              \
    } while(0)

#define end_read_counter2(h, i1, i2, v1, v2)                            \
    do {                                                                \
        reorder_barrier();                                              \
        v1 = *(volatile uint32_t *)(&((uint32_t*) h->counters)[i1 & counter_mask]); \
        v2 = *(volatile uint32_t *)(&((uint32_t*) h->counters)[i2 & counter_mask]); \
    } while (0)


/**
 * @brief Atomic increase the counter
 *
 */
#define start_incr_counter(h, idx)                                  \
    do {                                                            \
        ((volatile uint32_t *)h->counters)[idx & counter_mask]++;   \
        reorder_barrier();                                          \
    } while(0)

#define end_incr_counter(h, idx)                                    \
    do {                                                            \
        reorder_barrier();                                          \
        ((volatile uint32_t*) h->counters)[idx & counter_mask]++;   \
    } while(0)


#define start_incr_counter2(h, i1, i2)                                  \
    do {                                                                \
        if (likely((i1 & counter_mask) != (i2 & counter_mask))) {       \
            ((volatile uint32_t *)h->counters)[i1 & counter_mask]++;    \
            ((volatile uint32_t *)h->counters)[i2 & counter_mask]++;    \
        } else {                                                        \
            ((volatile uint32_t *)h->counters)[i1 & counter_mask]++;    \
        }                                                               \
        reorder_barrier();                                              \
    } while(0)

#define end_incr_counter2(h, i1, i2)                                    \
    do {                                                                \
        reorder_barrier();                                              \
        if (likely((i1 & counter_mask) != (i2 & counter_mask))) {       \
            ((volatile uint32_t *)h->counters)[i1 & counter_mask]++;    \
            ((volatile uint32_t *)h->counters)[i2 & counter_mask]++;    \
        } else {                                                        \
            ((volatile uint32_t *)h->counters)[i1 & counter_mask]++;    \
        }                                                               \
    } while(0)


// dga does not think we need this mfence in end_incr, because
// the current code will call pthread_mutex_unlock before returning
// to the caller;  pthread_mutex_unlock is a memory barrier:
// http://www.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap04.html#tag_04_11
// __asm__ __volatile("mfence" ::: "memory");


#define hashsize(n) ((uint32_t) 1 << n)
#define hashmask(n) (hashsize(n) - 1)

static inline  uint32_t _hashed_key(const char*  key,
                                    const size_t nkey) {
    return CityHash32(key, nkey);
}

/**
 * @brief Compute the index of the first bucket
 *
 * @param hv 32-bit hash value of the key
 *
 * @return The first bucket
 */
static inline size_t _index_hash(cuckoo_hashtable_t* h,
                                 const uint32_t hv) {
    return  hv & hashmask(h->hashpower);
}


/**
 * @brief Compute the index of the second bucket
 *
 * @param hv 32-bit hash value of the key
 * @param index The index of the first bucket
 *
 * @return  The second bucket
 */
static inline size_t _alt_index(cuckoo_hashtable_t* h,
                                const uint32_t hv,
                                const size_t index) {
    uint32_t tag = (hv >> 24) + 1; // ensure tag is nonzero for the multiply
    /* 0x5bd1e995 is the hash constant from MurmurHash2 */
    return (index ^ (tag * 0x5bd1e995)) & hashmask(h->hashpower);
}

/**
 * @brief access the j-th key in the i-th bucket
 *
 */
static inline char* slot_key(cuckoo_hashtable_t* h,
                              const size_t i,
                              const size_t j) {
    assert(i < hashsize(h->hashpower));
    assert(j < SLOT_PER_BUCKET);
    char* bucket = h->buckets + i * h->bucketsize;
    return bucket + h->nkey * j;
}

/**
 * @brief access the j-th value in the i-th bucket
 *
 */
static inline char* slot_val(cuckoo_hashtable_t* h,
                              const size_t i,
                              const size_t j) {
    assert(i < hashsize(h->hashpower));
    assert(j < SLOT_PER_BUCKET);
    char* bucket = h->buckets + i * h->bucketsize;
    return bucket + h->nkey * SLOT_PER_BUCKET + h->nval * j;
}

static inline int slot_flag(cuckoo_hashtable_t* h,
                            const size_t i,
                            const size_t j) {
    assert(i < hashsize(h->hashpower));
    assert(j < SLOT_PER_BUCKET);
    char* bucket = h->buckets + i * h->bucketsize;
    char* flag   = bucket + (h->nkey + h->nval) * SLOT_PER_BUCKET;
    return get_bit(flag, j);
}

static inline int bucket_flag(cuckoo_hashtable_t* h,
                              const size_t i) {
    assert(i < hashsize(h->hashpower));
    char* bucket = h->buckets + i * h->bucketsize;
    char* flag   = bucket + (h->nkey + h->nval) * SLOT_PER_BUCKET;
    return *flag;
}

static inline void slot_set_used(cuckoo_hashtable_t* h,
                                  const size_t i,
                                  const size_t j) {
    assert(i < hashsize(h->hashpower));
    assert(j < SLOT_PER_BUCKET);
    char* bucket = h->buckets + i * h->bucketsize;
    char* flag   = bucket + (h->nkey + h->nval) * SLOT_PER_BUCKET;
    set_bit(flag, j, 1);
}

static inline void slot_set_empty(cuckoo_hashtable_t* h,
                                   const size_t i,
                                   const size_t j) {
    assert(i < hashsize(h->hashpower));
    assert(j < SLOT_PER_BUCKET);
    char* bucket = h->buckets + i * h->bucketsize;
    char* flag   = bucket + (h->nkey + h->nval) * SLOT_PER_BUCKET;
    set_bit(flag, j, 0);
}

static inline bool is_slot_empty(cuckoo_hashtable_t* h,
                                 size_t i,
                                 size_t j) {
    if (0 == slot_flag(h, i, j)) {
        return true;
    }

    /*
     * even it shows "non-empty", it could be a false alert
     */
    if (h->expanding) {
        // when we are expanding
        // we could leave keys in their old but wrong buckets
        uint32_t hv = _hashed_key(slot_key(h, i, j), h->nkey);
        size_t   i1 = _index_hash(h, hv);
        size_t   i2 = _alt_index(h, hv, i1);

        if ((i != i1) && (i != i2)) {
            slot_set_empty(h, i, j);
            return true;
        }
    }
    return false;
}

typedef struct  {
    size_t   buckets[NUM_CUCKOO_PATH];
    size_t   slots[NUM_CUCKOO_PATH];
    uint32_t hvs[NUM_CUCKOO_PATH];
}  __attribute__((__packed__))
CuckooRecord;

/**
 * @brief Make bucket from[idx] slot[whichslot] available to insert a new item
 *
 * @param from:   the array of bucket index
 * @param whichslot: the slot available
 * @param  depth: the current cuckoo depth
 *
 * @return depth on success, -1 otherwise
 */
static int _cuckoopath_search(cuckoo_hashtable_t* h,
                              CuckooRecord* cuckoo_path,
                              size_t *cp_index,
                              size_t *num_kicks) {

    int depth = 0;
    while ((*num_kicks < MAX_CUCKOO_COUNT) &&
           (depth >= 0) &&
           (depth < MAX_CUCKOO_COUNT - 1)) {

        CuckooRecord *curr = cuckoo_path + depth;
        CuckooRecord *next = cuckoo_path + depth + 1;

        /*
         * Check if any slot is already free
         */
        for (size_t idx = 0; idx < NUM_CUCKOO_PATH; idx++) {
            size_t i, j;
            i = curr->buckets[idx];
            for (j = 0; j < SLOT_PER_BUCKET; j++) {
                if (is_slot_empty(h, i, j)) {
                    curr->slots[idx] = j;
                    *cp_index   = idx;
                    return depth;
                }
            }

            /* pick the victim as the j-th item */
            j = (cheap_rand() >> 20) % SLOT_PER_BUCKET;

            //memcpy(&curr->keys[idx], slot_key(h, i, j), h->nkey);
            uint32_t hv = _hashed_key(slot_key(h, i, j), h->nkey);
            curr->slots[idx] = j;
            curr->hvs[idx]   = hv;
            next->buckets[idx] = _alt_index(h, hv, i);
        }

        *num_kicks += NUM_CUCKOO_PATH;
        depth++;
    }

    DBG("%zu max cuckoo achieved, abort\n", *num_kicks);
    return -1;
}

static int _cuckoopath_move(cuckoo_hashtable_t* h,
                            CuckooRecord* cuckoo_path,
                            size_t depth,
                            size_t idx) {

    while (depth > 0) {

        /*
         * Move the key/value from buckets[i1] slot[j1] to buckets[i2] slot[j2]
         * buckets[i1] slot[j1] will be available after move
         */
        CuckooRecord *from = cuckoo_path + depth - 1;
        CuckooRecord *to   = cuckoo_path + depth;
        size_t i1 = from->buckets[idx];
        size_t j1 = from->slots[idx];
        size_t i2 = to->buckets[idx];
        size_t j2 = to->slots[idx];

        /*
         * We plan to kick out j1, but let's check if it is still there;
         * there's a small chance we've gotten scooped by a later cuckoo.
         * If that happened, just... try again.
         */
        //if (!keycmp(slot_key(h, i1, j1), (char*) &(from->keys[idx]))) {
        uint32_t hv = _hashed_key(slot_key(h, i1, j1), h->nkey);
        if (hv != from->hvs[idx]) {
            /* try again */
            return depth;
        }

        assert(is_slot_empty(h, i2, j2));

        start_incr_counter2(h, i1, i2);

        memcpy(slot_key(h, i2, j2), slot_key(h, i1, j1), h->nkey);
        memcpy(slot_val(h, i2, j2), slot_val(h, i1, j1), h->nval);


        slot_set_used(h, i2, j2);
        slot_set_empty(h, i1, j1);

        end_incr_counter2(h, i1, i2);

        depth--;
    }

    return depth;
}

static bool _run_cuckoo(cuckoo_hashtable_t* h,
                        const size_t i1,
                        const size_t i2,
                        size_t* i) {
#ifdef __linux__
    static __thread CuckooRecord* cuckoo_path = NULL;
#else
    /*
     * "__thread" is not supported by default on MacOS
     * malloc and free cuckoo_path on every call
     */
    CuckooRecord* cuckoo_path = NULL;
#endif
    if (!cuckoo_path) {
        cuckoo_path = malloc(MAX_CUCKOO_COUNT * sizeof(CuckooRecord));
        if (!cuckoo_path) {
            fprintf(stderr, "Failed to init cuckoo path.\n");
            return false;
        }
    }

    memset(cuckoo_path, 0, MAX_CUCKOO_COUNT * sizeof(CuckooRecord));

    for (size_t idx = 0; idx < NUM_CUCKOO_PATH; idx++) {
        if (idx < NUM_CUCKOO_PATH / 2) {
            cuckoo_path[0].buckets[idx] = i1;
        } else {
            cuckoo_path[0].buckets[idx] = i2;
        }
    }

    bool done = false;
    while (!done) {
        size_t num_kicks = 0;
        size_t idx = 0;

        int depth = _cuckoopath_search(h, cuckoo_path, &idx, &num_kicks);
        if (depth < 0) {
            break;
        }

        int curr_depth = _cuckoopath_move(h, cuckoo_path, depth, idx);
        if (0 == curr_depth) {
            *i = cuckoo_path[0].buckets[idx];
            done = true;
            break;
        }
    }

#ifdef __linux__
#else
    free(cuckoo_path);
#endif

    return done;
}


/**
 * @brief Try to read bucket i and check if the given key is there
 *
 * @param key The key to search
 * @param val The address to copy value to
 * @param i Index of bucket
 *
 * @return true if key is found, false otherwise
 */
static bool _try_read_from_bucket(cuckoo_hashtable_t* h,
                                  const char *key,
                                  char *val,
                                  const size_t i) {
    for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
        /* check if this slot is used */
        if (is_slot_empty(h, i, j)) {
            continue;
        }

        if (keycmp(key, slot_key(h, i, j), h->nkey)) {
            if (NULL != val) {
                memcpy(val, slot_val(h, i, j), h->nval);
            }
            return true;
        }
    }
    return false;
}

/**
 * @brief Try to add key/val to bucket i,
 *
 * @param key Pointer to the key to store
 * @param val Pointer to the value to store
 * @param i Bucket index
 *
 * @return true on success and false on failure
 */
static bool _try_add_to_bucket(cuckoo_hashtable_t* h,
                               const char* key,
                               const char* val,
                               size_t i) {
    for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
        if (is_slot_empty(h, i, j)) {

            start_incr_counter(h, i);

            memcpy(slot_key(h, i, j), key, h->nkey);
            memcpy(slot_val(h, i, j), val, h->nval);
            slot_set_used(h, i, j);

            end_incr_counter(h, i);
            h->hashitems++;
            return true;
        }
    }
    return false;
}


/**
 * @brief Try to delete key and its corresponding value from bucket i,
 *
 * @param key handler to the key to store
 * @param i Bucket index

 * @return true if key is found, false otherwise
 */
static bool _try_del_from_bucket(cuckoo_hashtable_t* h,
                                 const char*  key,
                                 const size_t i) {
    for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
        /* check if this slot is used */
        if (is_slot_empty(h, i, j)) {
            continue;
        }

        if (keycmp(slot_key(h, i, j), key, h->nkey)) {

            start_incr_counter(h, i);
            slot_set_empty(h, i, j);
            end_incr_counter(h, i);

            h->hashitems--;
            return true;
        }
    }
    return false;
}


/**
 * @brief internal of cuckoo_find
 *
 * @param key handler to the key to search
 * @param val handler to the value to return
 * @param i1  1st bucket index
 * @param i2  2nd bucket index
 *
 * @return
 */
static cuckoo_status _cuckoo_find(cuckoo_hashtable_t* h,
                                  const char *key,
                                  char *val,
                                  const size_t i1,
                                  const size_t i2) {
    bool result;

    uint32_t vs1, vs2, ve1, ve2;
TryRead:
    start_read_counter2(h, i1, i2, vs1, vs2);

    if (((vs1 & 1) || (vs2 & 1) )) {
        goto TryRead;
    }

    result = _try_read_from_bucket(h, key, val, i1);
    if (!result) {
        result = _try_read_from_bucket(h, key, val, i2);
    }

    end_read_counter2(h, i1, i2, ve1, ve2);

    if (((vs1 != ve1) || (vs2 != ve2))) {
        goto TryRead;
    }

    if (result) {
        return ok;
    } else {
        return failure_key_not_found;
    }
}

/*
 * the lock is acquired by the caller
 */
static cuckoo_status _cuckoo_insert(cuckoo_hashtable_t* h,
                                    const char*  key,
                                    const char*  val,
                                    const size_t i1,
                                    const size_t i2) {

    /*
     * try to add new key to bucket i1 first, then try bucket i2
     */
    if (_try_add_to_bucket(h, key, val, i1)) {
        return ok;
    }

    if (_try_add_to_bucket(h, key, val, i2)) {
        return ok;
    }

    /*
     * we are unlucky, so let's perform cuckoo hashing
     */
    size_t i = 0;

    if (_run_cuckoo(h, i1, i2, &i)) {
        if (_try_add_to_bucket(h, key, val, i)) {
            return ok;
        }
    }

    DBG("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
        h->hashpower, h->hashitems, 1.0 * h->hashitems / SLOT_PER_BUCKET / hashsize(h->hashpower));

    return failure_table_full;
}

/*
 * the lock is acquired by the caller
 */
static cuckoo_status _cuckoo_delete(cuckoo_hashtable_t* h,
                                    const char*  key,
                                    const size_t i1,
                                    const size_t i2) {
    if (_try_del_from_bucket(h, key, i1)) {
        return ok;
    }

    if (_try_del_from_bucket(h, key, i2)) {
        return ok;
    }

    return failure_key_not_found;
}


/*
 * the lock is acquired by the caller
 */
static cuckoo_status _cuckoo_update(cuckoo_hashtable_t* h,
                                    const char*  key,
                                    const char*  val,
                                    const size_t i1,
                                    const size_t i2) {
    cuckoo_status st;

    st = _cuckoo_delete(h, key, i1, i2);
    if (ok != st) {
        return st;
    }

    st = _cuckoo_insert(h, key, val, i1, i2);

    return st;
}

/*
 * the lock is acquired by the caller
 */
static void _cuckoo_clean(cuckoo_hashtable_t* h,
                          const size_t size) {
    for (size_t ii = 0; ii < size; ii++) {
        size_t i = h->cleaned_buckets;
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            if (0 == slot_flag(h, i, j)) {
                continue;
            }
            uint32_t hv = _hashed_key(slot_key(h, i, j), h->nkey);
            size_t   i1 = _index_hash(h, hv);
            size_t   i2 = _alt_index(h, hv, i1);

            if ((i != i1) && (i != i2)) {
                //DBG("delete key %u , i=%zu i1=%zu i2=%zu\n", slot_key(h, i, j), i, i1, i2);
                slot_set_empty(h, i, j);
            }
        }
        h->cleaned_buckets++;
        if (h->cleaned_buckets == hashsize((h->hashpower))) {
            h->expanding = false;
            DBG("table clean done, cleaned_buckets = %zu\n", h->cleaned_buckets);
            return;
        }
    }
    //DBG("_cuckoo_clean: cleaned_buckets = %zu\n", h->cleaned_buckets);
}


/********************************************************************
 *               Interface of cuckoo hash table
 *********************************************************************/

cuckoo_hashtable_t* cuckoo_init(const int hashtable_init,
                                const size_t nkey,
                                const size_t nval) {
    cuckoo_hashtable_t* h = (cuckoo_hashtable_t*) malloc(sizeof(cuckoo_hashtable_t));
    if (!h) {
        goto Cleanup;
    }

    h->hashpower  = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
    h->nkey       = nkey;
    h->nval       = nval;
    h->bucketsize = (h->nkey + h->nval) * SLOT_PER_BUCKET + (SLOT_PER_BUCKET + 7) / 8;
    h->tablesize  = h->bucketsize * hashsize(h->hashpower);
    h->hashitems  = 0;
    h->expanding  = false;
    pthread_mutex_init(&h->lock, NULL);

    h->buckets = malloc(h->tablesize);
    if (!h->buckets) {
        fprintf(stderr, "Failed to init hashtable.\n");
        goto Cleanup;
    }

    h->counters = malloc(counter_size * sizeof(uint32_t));
    if (!h->counters) {
        fprintf(stderr, "Failed to init counter array.\n");
        goto Cleanup;
    }

    cuckoo_clear(h);

    return h;

Cleanup:
    if (h) {
        free(h->counters);
        free(h->buckets);
    }
    free(h);
    return NULL;

}

cuckoo_status cuckoo_exit(cuckoo_hashtable_t* h) {
    assert(h != NULL);
    pthread_mutex_destroy(&h->lock);
    free(h->buckets);
    free(h->counters);
    free(h);
    return ok;
}

cuckoo_status cuckoo_clear(cuckoo_hashtable_t* h) {

    mutex_lock(&h->lock);

    memset(h->buckets,  0, h->tablesize);
    memset(h->counters, 0, counter_size * sizeof(uint32_t));
    h->expanding = false;

    mutex_unlock(&h->lock);
    return ok;
}

cuckoo_status cuckoo_find(cuckoo_hashtable_t* h,
                          const char *key,
                          char *val) {

    uint32_t hv = _hashed_key(key, h->nkey);

    //
    // potential "false read miss" under expansion:
    // calculation of i1 and i2 may use stale hashpower
    //
    size_t   i1 = _index_hash(h, hv);
    size_t   i2 = _alt_index(h, hv, i1);

    cuckoo_status st = _cuckoo_find(h, key, val, i1, i2);

    return st;
}

cuckoo_status cuckoo_insert(cuckoo_hashtable_t* h,
                            const char *key,
                            const char* val) {

    uint32_t hv = _hashed_key(key, h->nkey);

    mutex_lock(&h->lock);

    size_t   i1 = _index_hash(h, hv);
    size_t   i2 = _alt_index(h, hv, i1);

    cuckoo_status st = _cuckoo_find(h, key, (char*) NULL, i1, i2);
    if (ok == st) {
        mutex_unlock(&h->lock);
        return failure_key_duplicated;
    }

    st = _cuckoo_insert(h, key, val, i1, i2);

    if (h->expanding) {
        /*
         * still some work to do before releasing the lock
         */
        _cuckoo_clean(h, DEFAULT_BULK_CLEAN);
    }

    mutex_unlock(&h->lock);

    return st;
}

cuckoo_status cuckoo_delete(cuckoo_hashtable_t* h,
                            const char *key) {

    uint32_t hv = _hashed_key(key, h->nkey);

    mutex_lock(&h->lock);

    size_t   i1 = _index_hash(h, hv);
    size_t   i2 = _alt_index(h, hv, i1);

    cuckoo_status st = _cuckoo_delete(h, key, i1, i2);

    mutex_unlock(&h->lock);

    return st;
}


cuckoo_status cuckoo_update(cuckoo_hashtable_t* h,
                            const char *key,
                            const char *val) {

    uint32_t hv = _hashed_key(key, h->nkey);

    mutex_lock(&h->lock);

    size_t   i1 = _index_hash(h, hv);
    size_t   i2 = _alt_index(h, hv, i1);

    cuckoo_status st = _cuckoo_update(h, key, val, i1, i2);

    mutex_unlock(&h->lock);

    return st;
}

cuckoo_status cuckoo_expand(cuckoo_hashtable_t* h) {

    mutex_lock(&h->lock);
    if (h->expanding) {
        mutex_unlock(&h->lock);
        //DBG("expansion is on-going\n", NULL);
        return failure_under_expansion;
    }

    h->expanding = true;

    void* old_buckets = h->buckets;
    void* new_buckets = malloc(h->tablesize * 2);
    printf("current: power %zu, tablesize %zu\n", h->hashpower, h->tablesize);
    printf("[%p-%p]\n", new_buckets, new_buckets + h->tablesize * 2);
    if (!new_buckets) {
        h->expanding = false;
        mutex_unlock(&h->lock);
        return failure_space_not_enough;
    }

    memcpy(new_buckets, h->buckets, h->tablesize);
    memcpy(new_buckets + h->tablesize, h->buckets, h->tablesize);

    h->buckets = new_buckets;

    reorder_barrier();

    h->hashpower++;
    h->tablesize <<= 1;
    h->cleaned_buckets = 0;
    printf("old %p, new %p\n", old_buckets, new_buckets);
    mutex_unlock(&h->lock);

    free(old_buckets);

    return ok;
}

void cuckoo_report(cuckoo_hashtable_t* h) {

    printf("total number of items %zu\n", h->hashitems);
    printf("total size %zu Bytes, or %.2f MB\n", 
           h->tablesize, (float) h->tablesize / (1 << 20));
    printf("load factor %.4f\n", 
           1.0 * h->hashitems / SLOT_PER_BUCKET / hashsize(h->hashpower));
}

float cuckoo_loadfactor(cuckoo_hashtable_t* h) {
    return 1.0 * h->hashitems / SLOT_PER_BUCKET / hashsize(h->hashpower);
}
