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

#include "cuckoohash.h"

/*
 * default hash table size
 */
#define HASHPOWER_DEFAULT 16

/*
 * The maximum number of cuckoo operations per insert,
 */
#define MAX_CUCKOO_COUNT 500

/*
 * The number of cuckoo paths
 */
#define NUM_CUCKOO_PATH 2

#define  keyver_count ((uint32_t)1 << (13))
#define  keyver_mask  (keyver_count - 1)


/*
 * the structure of a bucket
 */
#define bucketsize 4
typedef struct {
    KeyType keys[bucketsize];
    ValType vals[bucketsize];
}  __attribute__((__packed__))
Bucket;


/* /\** */
/*  *  @brief Atomic read the counter */
/*  * */
/*  *\/ */
/* #define read_keyver(h, idx)                                      \ */
/*     __sync_fetch_and_add(&((uint32_t*) h->keyver_array)[idx & keyver_mask], 0) */

/* /\** */
/*  * @brief Atomic increase the counter */
/*  * */
/*  *\/ */
/* #define incr_keyver(h, idx)                                      \ */
/*     __sync_fetch_and_add(&((uint32_t*) h->keyver_array)[idx & keyver_mask], 1) */

/**
 *  @brief read the counter, ensured by x86 memory ordering model
 *
 */
#define start_read_keyver(h, idx, result)                               \
    do {                                                                \
        result = *(volatile uint32_t *)(&((uint32_t*) h->keyver_array)[idx & keyver_mask]); \
        __asm__ __volatile__("" ::: "memory");                          \
    } while(0)

#define end_read_keyver(h, idx, result)                                 \
    do {                                                                \
        __asm__ __volatile__("" ::: "memory");                          \
        result = *(volatile uint32_t *)(&((uint32_t*) h->keyver_array)[idx & keyver_mask]); \
    } while (0)

/**
 * @brief Atomic increase the counter
 *
 */
#define start_incr_keyver(h, idx)                                       \
    do {                                                                \
        ((volatile uint32_t *)h->keyver_array)[idx & keyver_mask] += 1; \
        __asm__ __volatile__("" ::: "memory");                          \
    } while(0)

#define end_incr_keyver(h, idx)                                         \
    do {                                                                \
        __asm__ __volatile__("" ::: "memory");                          \
        ((volatile uint32_t*) h->keyver_array)[idx & keyver_mask] += 1; \
    } while(0)

// dga does not thing we need this mfence in end_incr, because
// the current code will call pthread_mutex_unlock before returning
// to the caller;  pthread_mutex_unlock is a memory barrier:
// http://www.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap04.html#tag_04_11
// __asm__ __volatile("mfence" ::: "memory");                      \


static inline  uint32_t _hashed_key(const char* key) {
    return CityHash32(key, sizeof(KeyType));
}

#define hashsize(n) ((uint32_t) 1 << n)
#define hashmask(n) (hashsize(n) - 1)



/**
 * @brief Compute the index of the first bucket
 *
 * @param hv 32-bit hash value of the key
 *
 * @return The first bucket
 */
static inline size_t _index_hash(cuckoo_hashtable_t* h,
                                 const uint32_t hv) {
//    return  (hv >> (32 - h->hashpower));
    return  (hv & hashmask(h->hashpower));
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
    // 0x5bd1e995 is the hash constant from MurmurHash2
    //uint32_t tag = hv & 0xFF;
    uint32_t tag = hv >> 24;
    return (index ^ (tag * 0x5bd1e995)) & hashmask(h->hashpower);
    //return (hv ^ (tag * 0x5bd1e995)) & hashmask(h->hashpower);
    //return ((hv >> 32) & hashmask(h->hashpower));
}

/**
 * @brief Compute the index of the corresponding counter in keyver_array
 *
 * @param hv 32-bit hash value of the key
 *
 * @return The index of the counter
 */
static inline size_t _lock_index(const uint32_t hv) {
    return hv & keyver_mask;
}


#define TABLE_KEY(h, i, j) ((Bucket*) h->buckets)[i].keys[j]
#define TABLE_VAL(h, i, j) ((Bucket*) h->buckets)[i].vals[j]


//#define IS_SLOT_EMPTY(h, i, j) (TABLE_KEY(h, i, j)==0)
static inline bool is_slot_empty(cuckoo_hashtable_t* h,
                                 size_t i,
                                 size_t j) {
    if (TABLE_KEY(h, i, j)==0) {
        return true;
    }

    if (h->expanding) {
        // when we are expanding
        // we could leave keys in their old but wrong buckets
        uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i, j));
        size_t i1 = _index_hash(h, hv);
        size_t i2 = _alt_index(h, hv, i1);
        if ((i != i1) && (i != i2)) {
            TABLE_KEY(h, i, j)=0;
            return true;
        }
    }
    return false;
}



typedef struct  {
    size_t buckets[NUM_CUCKOO_PATH];
    size_t slots[NUM_CUCKOO_PATH];
    KeyType keys[NUM_CUCKOO_PATH];
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
        size_t idx;
        for (idx = 0; idx < NUM_CUCKOO_PATH; idx++) {
            size_t i;
            size_t j;
            i = curr->buckets[idx];
            for (j = 0; j < bucketsize; j++) {
                if (is_slot_empty(h, i, j)) {
                    curr->slots[idx] = j;
                    *cp_index   = idx;
                    return depth;
                }
            }

            /* pick the victim as the j-th item */
            j = (cheap_rand() >> 20) % bucketsize;

            curr->slots[idx] = j;
            curr->keys[idx]  = TABLE_KEY(h, i, j);
            uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i, j));
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
         * Move the key/value in  buckets[i1] slot[j1] to buckets[i2] slot[j2]
         * and make buckets[i1] slot[j1] available
         *
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
        if (!keycmp((char*) &TABLE_KEY(h, i1, j1), (char*) &(from->keys[idx]))) {
            /* try again */
            return depth;
        }

        //assert(is_slot_empty(h, i2, j2));

        uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i1, j1));
        size_t keylock   = _lock_index(hv);

        start_incr_keyver(h, keylock);

        TABLE_KEY(h, i2, j2) = TABLE_KEY(h, i1, j1);
        TABLE_VAL(h, i2, j2) = TABLE_VAL(h, i1, j1);
        TABLE_KEY(h, i1, j1) = 0;
        TABLE_VAL(h, i1, j1) = 0;

        end_incr_keyver(h, keylock);

        depth --;
    }

    return depth;

}

static bool _run_cuckoo(cuckoo_hashtable_t* h,
                        size_t i1,
                        size_t i2,
                        size_t* i) {

    CuckooRecord* cuckoo_path;

    cuckoo_path = malloc(MAX_CUCKOO_COUNT * sizeof(CuckooRecord));
    if (! cuckoo_path) {
        fprintf(stderr, "Failed to init cuckoo path.\n");
        return -1;
    }
    memset(cuckoo_path, 0, MAX_CUCKOO_COUNT * sizeof(CuckooRecord));

    size_t idx;
    for (idx = 0; idx < NUM_CUCKOO_PATH; idx++) {
        if (idx < NUM_CUCKOO_PATH / 2) {
            cuckoo_path[0].buckets[idx] = i1;
        } else {
            cuckoo_path[0].buckets[idx] = i2;
        }
    }

    size_t num_kicks = 0;
    while (1) {
        int depth;
        depth = _cuckoopath_search(h, cuckoo_path, &idx, &num_kicks);
        if (depth < 0) {
            break;
        }

        int curr_depth = _cuckoopath_move(h, cuckoo_path, depth, idx);
        if (curr_depth == 0) {
            *i = cuckoo_path[0].buckets[idx];
            free(cuckoo_path);
            return true;
        }
    }
    free(cuckoo_path);
    return false;
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
                                  size_t i) {
    for (size_t j = 0; j < bucketsize; j++) {

        if (keycmp((char*) &TABLE_KEY(h, i, j), key)) {
            memcpy(val, (char*) &TABLE_VAL(h, i, j), sizeof(ValType));
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
 * @param keylock The index of key version counter
 *
 * @return true on success and false on failure
 */
static bool _try_add_to_bucket(cuckoo_hashtable_t* h,
                               const char* key,
                               const char* val,
                               size_t i,
                               size_t keylock) {
    for (size_t j = 0; j < bucketsize; j++) {
        if (is_slot_empty(h, i, j)) {

            start_incr_keyver(h, keylock);

            memcpy(&TABLE_KEY(h, i, j), key, sizeof(KeyType));
            memcpy(&TABLE_VAL(h, i, j), val, sizeof(ValType));

            end_incr_keyver(h, keylock);
            h->hashitems++;
            return true;
        }
    }
    return false;
}




/**
 * @brief Try to delete key and its corresponding value from bucket i,
 *
 * @param key Pointer to the key to store
 * @param i Bucket index
 * @param keylock The index of key version counter

 * @return true if key is found, false otherwise
 */
static bool _try_del_from_bucket(cuckoo_hashtable_t* h,
                                 const char*key,
                                 size_t i,
                                 size_t keylock) {
    for (size_t j = 0; j < bucketsize; j++) {

        if (keycmp((char*) &TABLE_KEY(h, i, j), key)) {

            start_incr_keyver(h, keylock);

            TABLE_KEY(h, i, j) = 0;
            TABLE_VAL(h, i, j) = 0;

            end_incr_keyver(h, keylock);
            h->hashitems --;
            return true;
        }
    }
    return false;
}


/**
 * @brief internal of cuckoo_find
 *
 * @param key
 * @param val
 * @param i1
 * @param i2
 * @param keylock
 *
 * @return
 */
static cuckoo_status _cuckoo_find(cuckoo_hashtable_t* h,
                                  const char *key,
                                  char *val,
                                  size_t i1,
                                  size_t i2,
                                  size_t keylock) {
    bool result;

    uint32_t vs, ve;
TryRead:
    start_read_keyver(h, keylock, vs);

    result = _try_read_from_bucket(h, key, val, i1);
    if (!result) {
        result = _try_read_from_bucket(h, key, val, i2);
    }

    end_read_keyver(h, keylock, ve);

    if (vs & 1 || vs != ve) {
        goto TryRead;
    }

    if (result) {
        return ok;
    } else {
        return failure_key_not_found;
    }
}

static cuckoo_status _cuckoo_insert(cuckoo_hashtable_t* h,
                                    const char* key,
                                    const char* val,
                                    size_t i1,
                                    size_t i2,
                                    size_t keylock) {

    /*
     * try to add new key to bucket i1 first, then try bucket i2
     */
    if (_try_add_to_bucket(h, key, val, i1, keylock)) {
        return ok;
    }

    if (_try_add_to_bucket(h, key, val, i2, keylock)) {
        return ok;
    }


    /*
     * we are unlucky, so let's perform cuckoo hashing
     */
    size_t i = 0;
            
    if (_run_cuckoo(h, i1, i2, &i)) {
        if (_try_add_to_bucket(h, key, val, i, keylock)) {
            return ok;
        }
    }

    DBG("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
        h->hashpower, h->hashitems, 1.0 * h->hashitems / bucketsize / hashsize(h->hashpower));


    return failure_table_full;

}

static cuckoo_status _cuckoo_delete(cuckoo_hashtable_t* h,
                                    const char* key,
                                    size_t i1,
                                    size_t i2,
                                    size_t keylock) {
    if (_try_del_from_bucket(h, key, i1, keylock)) {
        return ok;
    }

    if (_try_del_from_bucket(h, key, i2, keylock)) {
        return ok;
    }

    return failure_key_not_found;

}

static void _cuckoo_clean(cuckoo_hashtable_t* h, size_t size) {
    for (size_t ii = 0; ii < size; ii++) {
        size_t i = h->cleaned_buckets;
        for (size_t j = 0; j < bucketsize; j++) {
            if (TABLE_KEY(h, i, j) == 0) {
                continue;
            }
            uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i, j));
            size_t i1 = _index_hash(h, hv);
            size_t i2 = _alt_index(h, hv, i1);
            if ((i != i1) && (i != i2)) {
                //DBG("delete key %u , i=%zu i1=%zu i2=%zu\n", TABLE_KEY(h, i, j), i, i1, i2);

                TABLE_KEY(h, i, j) = 0;
                TABLE_VAL(h, i, j) = 0;
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

cuckoo_hashtable_t* cuckoo_init(const int hashtable_init) {
    cuckoo_hashtable_t* h = (cuckoo_hashtable_t*) malloc(sizeof(cuckoo_hashtable_t));
    if (!h) {
        goto Cleanup;
    }

    h->hashpower  = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
    h->hashitems  = 0;
    h->expanding  = false;
    pthread_mutex_init(&h->lock, NULL);

    h->buckets = malloc(hashsize(h->hashpower) * sizeof(Bucket));
    if (! h->buckets) {
        fprintf(stderr, "Failed to init hashtable.\n");
        goto Cleanup;
    }

    h->keyver_array = malloc(keyver_count * sizeof(uint32_t));
    if (! h->keyver_array) {
        fprintf(stderr, "Failed to init key version array.\n");
        goto Cleanup;
    }


    memset(h->buckets, 0, hashsize(h->hashpower) * sizeof(Bucket));
    memset(h->keyver_array, 0, keyver_count * sizeof(uint32_t));

    return h;

Cleanup:
    if (h) {
        free(h->keyver_array);
        free(h->buckets);
    }
    free(h);
    return NULL;

}

cuckoo_status cuckoo_exit(cuckoo_hashtable_t* h) {
    pthread_mutex_destroy(&h->lock);
    free(h->buckets);
    free(h->keyver_array);
    free(h);
    return ok;
}

cuckoo_status cuckoo_find(cuckoo_hashtable_t* h,
                          const char *key,
                          char *val) {

    uint32_t hv    = _hashed_key(key);
    size_t i1      = _index_hash(h, hv);
    size_t i2      = _alt_index(h, hv, i1);
    size_t keylock = _lock_index(hv);

    cuckoo_status st = _cuckoo_find(h, key, val, i1, i2, keylock);

    if (st == failure_key_not_found) {
        DBG("miss for key %u i1=%zu i2=%zu hv=%u\n", *((KeyType*) key), i1, i2, hv);
    }

    return st;
}

cuckoo_status cuckoo_insert(cuckoo_hashtable_t* h,
                            const char *key,
                            const char* val) {
    mutex_lock(&h->lock);

    uint32_t hv = _hashed_key(key);
    size_t i1   = _index_hash(h, hv);
    size_t i2   = _alt_index(h, hv, i1);
    size_t keylock = _lock_index(hv);

    ValType oldval;
    cuckoo_status st;

    st = _cuckoo_find(h, key, (char*) &oldval, i1, i2, keylock);
    if  (st == ok) {
        mutex_unlock(&h->lock);
        return failure_key_duplicated;
    }

    st =  _cuckoo_insert(h, key, val, i1, i2, keylock);

    if (h->expanding) {
        // still some work to do
        _cuckoo_clean(h, DEFAULT_BULK_CLEAN);
    }

    mutex_unlock(&h->lock);

    return st;
}

cuckoo_status cuckoo_delete(cuckoo_hashtable_t* h,
                            const char *key) {

    mutex_lock(&h->lock);

    uint32_t hv = _hashed_key(key);
    size_t i1   = _index_hash(h, hv);
    size_t i2   = _alt_index(h, hv, i1);
    size_t keylock = _lock_index(hv);

    cuckoo_status st;

    st = _cuckoo_delete(h, key, i1, i2, keylock);

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

    Bucket* old_buckets = (Bucket*) h->buckets;
    Bucket* new_buckets = (Bucket*) malloc(hashsize((h->hashpower + 1)) * sizeof(Bucket));
    if (!new_buckets) {
        h->expanding = false;
        mutex_unlock(&h->lock);
        return failure_space_not_enough;
    }

    memcpy(new_buckets, h->buckets, hashsize(h->hashpower) * sizeof(Bucket));
    memcpy(new_buckets + hashsize(h->hashpower), h->buckets, hashsize(h->hashpower) * sizeof(Bucket));


    h->buckets = new_buckets;
    h->hashpower++;
    h->cleaned_buckets = 0;

    //h->expanding = false;
    //_cuckoo_clean(h, hashsize(h->hashpower));

    mutex_unlock(&h->lock);

    free(old_buckets);

    return ok;
}

void cuckoo_report(cuckoo_hashtable_t* h) {

    size_t sz;
    sz = sizeof(Bucket) * hashsize(h->hashpower);
    DBG("total number of items %zu\n", h->hashitems);
    DBG("total size %zu Bytes, or %.2f MB\n", sz, (float) sz / (1 <<20));
    DBG("load factor %.4f\n", 1.0 * h->hashitems / bucketsize / hashsize(h->hashpower));
}

float cuckoo_loadfactor(cuckoo_hashtable_t* h) {
    return 1.0 * h->hashitems / bucketsize / hashsize(h->hashpower);
}
