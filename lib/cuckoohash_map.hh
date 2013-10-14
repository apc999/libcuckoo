#ifndef _CUCKOOHASH_MAP_HH
#define _CUCKOOHASH_MAP_HH
#include <functional>  // for std::hash
#include <utility>     // for std::pair
#include <stdexcept>
#include <cstring>
#include <cassert>
#include <pthread.h>
#include <memory>
#include <atomic>
#include <thread>
#include <chrono>

#include "cuckoohash_config.h"
#include "cuckoohash_util.h"
#include "util.h"
extern "C" {
#include "city.h"
}

// state used internally
typedef enum {
    ok = 0,
    failure = 1,
    failure_key_not_found = 2,
    failure_key_duplicated = 3,
    failure_space_not_enough = 4,
    failure_function_not_supported = 5,
    failure_table_full = 6,
    failure_under_expansion = 7,
} cuckoo_status;

// Forward-declares the iterator types, since they are needed in the
// map
template <class K, class V, class H> class c_iterator;
template <class K, class V, class H> class mut_iterator;

// This includes implementations for all functions that don't use
// iterators
template <class Key, class T, class Hash = std::hash<Key> >
class cuckoohash_map {

public:
    // Type definitions
    typedef Key               key_type;
    typedef std::pair<Key, T> value_type;
    typedef T                 mapped_type;
    typedef Hash              hasher;
    typedef size_t            size_type;

    // Constructor and destructor
    explicit cuckoohash_map(size_t hashpower_init = HASHPOWER_DEFAULT) {
        cuckoo_init(hashpower_init);
    }

    ~cuckoohash_map() {
        pthread_mutex_destroy(&lock_);
        TableInfo* ti = table_info.load();
        if (ti != nullptr) {
            delete table_info.load();
        }
    }

    void clear() {
        cuckoo_clear();
    }

    // Functions concerning size
    size_type size() const {
        return table_info.load()->hashitems_.load();
    }

    size_type max_size() const {
        assert(false);
        return 0;
    }

    bool empty() const {
        return (table_info.load()->hashitems_.load() == 0);
    }

    size_type bucket_count() const {
        return hashsize(table_info.load()->hashpower_);
    }

    size_type max_bucket_count() const {
        assert(false);
        return 0;
    }

    // Load factor
    float load_factor() const {
        TableInfo* ti = table_info.load();
        return 1.0 * ti->hashitems_.load() / SLOT_PER_BUCKET / hashsize(table_info.load()->hashpower_);
    }

    float max_load_factor() const {
        return 1.0;
    }

    // Lookup routines
    bool find(const key_type& key, mapped_type& val) {
        if (size() == 0) return false;

        const TableInfo* ti = table_info.load();
        uint32_t hv = hashed_key(key);
        size_t   i1 = index_hash(ti,hv);
        size_t   i2 = alt_index(ti, hv, i1);

        uint32_t vs1, vs2, ve1, ve2;
    TryRead:
        start_read_counter2(ti, i1, i2, vs1, vs2);

        if (((vs1 & 1) || (vs2 & 1) )) {
            goto TryRead;
        }

        cuckoo_status st = cuckoo_find(ti, key, val, i1, i2);

        end_read_counter2(ti, i1, i2, ve1, ve2);

        if (((vs1 != ve1) || (vs2 != ve2))) {
            goto TryRead;
        }

        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    value_type find(const key_type& key) {
        if (size() == 0) throw std::out_of_range("hashtable is empty");

        mapped_type val;

        bool done = find(key, val);

        if (done) {
            return value_type(key, val);
        } else {
            throw std::out_of_range("key not found in table");
        }
    }

    // Insertion routines
    bool insert(const key_type& key, const mapped_type& val) {

        uint32_t hv = hashed_key(key);
        mutex_lock(&lock_);

        TableInfo* ti = table_info.load();

        size_t   i1 = index_hash(ti, hv);
        size_t   i2 = alt_index(ti, hv, i1);

        cuckoo_status st;
        mapped_type oldval;

        st = cuckoo_find(ti, key, oldval, i1, i2);
        if (ok == st) {
            mutex_unlock(&lock_);
            return false; // failure_key_duplicated;
        }


        st = cuckoo_insert(ti, key, val, i1, i2);

        // If the table is full, it expands and tries again
        if (st == failure_table_full) {
            expanding_.store(true);
            if (cuckoo_expand_simple() == failure_under_expansion) {
                DBG("expansion is on-going\n", NULL);
                return false;
            }
            expanding_.store(false);
            ti = table_info.load();
            i1 = index_hash(ti, hv);
            i2 = alt_index(ti, hv, i1);
            st = cuckoo_insert(ti, key, val, i1, i2);
        }

        if (expanding_.load()) {
            /*
             * still some work to do before releasing the lock
             */
            cuckoo_clean(ti, DEFAULT_BULK_CLEAN);
        }

        mutex_unlock(&lock_);

        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    // Deletion routines
    bool erase(const key_type& key) {
        uint32_t hv = hashed_key(key);

        mutex_lock(&lock_);
        TableInfo* ti = table_info.load();
        size_t   i1 = index_hash(ti, hv);
        size_t   i2 = alt_index(ti, hv, i1);

        cuckoo_status st = cuckoo_delete(ti, key, i1, i2);

        mutex_unlock(&lock_);

        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    // None standard functions:

    bool expand() {
        mutex_lock(&lock_);
        expanding_.store(true);
        cuckoo_status st = cuckoo_expand();
        mutex_unlock(&lock_);
        return (st == ok);
    }

    void report() {
        mutex_lock(&lock_);
        cuckoo_report();
        mutex_unlock(&lock_);
    }

private:

    typedef struct {
        char        flag;
        key_type    keys[SLOT_PER_BUCKET];
        mapped_type vals[SLOT_PER_BUCKET];
    } __attribute__((__packed__)) Bucket;

    /* We store all of this info in one struct, so that all the data
     * can be atomically swapped */
    struct TableInfo {
        /* size of the table in bytes */
        size_t tablesize_;

        /* number of items inserted */
        std::atomic<size_t> hashitems_;

        /* 2**hashpower is the number of buckets */
        volatile size_t hashpower_;

        /* unique pointer to the array of buckets */
        std::unique_ptr<Bucket[]> buckets_;

        /* the unique_ptr array of version counters
         * we keep keyver_count = 8192
         */
        std::unique_ptr<uint32_t[]> counters_;
    };
    std::atomic<TableInfo*> table_info;

    /* Atomic operations on the version counters */

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

    /* key size in bytes */
    static const size_t kKeySize   = sizeof(key_type);

    /* value size in bytes */
    static const size_t kValueSize = sizeof(mapped_type);

    /* size of a bucket in bytes */
    static const size_t kBucketSize = sizeof(Bucket);

    /* size of counter array */
    static const size_t kNumCounters = 1 << 13;

    /* the mutex to serialize insert, delete, expand */
    pthread_mutex_t lock_;

    /* denoting if the table is doing expanding */
    std::atomic<bool> expanding_;

    /* number of buckets has been cleaned */
    size_t cleaned_buckets_;


    uint32_t hashed_key(const key_type &key) {
        // return hasher()(key);
        return CityHash32((const char*) &key, kKeySize);
    }

    /**
     * @brief Compute the index of the first bucket
     *
     * @param hv 32-bit hash value of the key
     *
     * @return The first bucket
     */
    size_t index_hash(const TableInfo* ti, const uint32_t hv) const {
        return hv & hashmask(ti->hashpower_);
    }


    /**
     * @brief Compute the index of the second bucket
     *
     * @param hv 32-bit hash value of the key
     * @param index The index of the first bucket
     *
     * @return  The second bucket
     */
    size_t alt_index(const TableInfo* ti,
                     const uint32_t hv,
                     const size_t index) const {
        uint32_t tag = (hv >> 24) + 1; // ensure tag is nonzero for the multiply
        /* 0x5bd1e995 is the hash constant from MurmurHash2 */
        return (index ^ (tag * 0x5bd1e995)) & hashmask(ti->hashpower_);
    }


    bool get_slot_flag(const TableInfo* ti, const size_t i, const size_t j) const {
        assert(i < hashsize(ti->hashpower_));
        assert(j < SLOT_PER_BUCKET);
        return get_bit(&(ti->buckets_[i].flag), j);
    }

    void set_slot_used(const TableInfo* ti, const size_t i, const size_t j) {
        assert(i < hashsize(ti->hashpower_));
        assert(j < SLOT_PER_BUCKET);
        set_bit(&(ti->buckets_[i].flag), j, 1);
    }

    void set_slot_empty(const TableInfo* ti, const size_t i, const size_t j) {
        assert(i < hashsize(ti->hashpower_));
        assert(j < SLOT_PER_BUCKET);
        set_bit(&(ti->buckets_[i].flag), j, 0);
    }

    bool is_slot_empty(const TableInfo* ti, const size_t i,const size_t j) {
        if (0 == get_slot_flag(ti, i, j)) {
            return true;
        }

        // even it shows "non-empty", it could be a false alert during
        // expansion
        if (expanding_.load() && i >= cleaned_buckets_) {
            // when we are expanding
            // we could leave keys in their old but wrong buckets
            uint32_t hv = hashed_key(ti->buckets_[i].keys[j]);
            size_t   i1 = index_hash(ti, hv);
            size_t   i2 = alt_index(ti, hv, i1);

            if ((i != i1) && (i != i2)) {
                set_slot_empty(ti, i, j);
                return true;
            }
        }
        return false;
    }

    typedef struct  {
        size_t   buckets[NUM_CUCKOO_PATH];
        size_t   slots[NUM_CUCKOO_PATH];
        key_type keys[NUM_CUCKOO_PATH];
    }  CuckooRecord; //__attribute__((__packed__)) CuckooRecord;

    /**
     * @brief Find a cuckoo path connecting to an empty slot
     *
     * @param cuckoo_path: cuckoo path
     * @param cp_index: which cuckoo path is good
     * @param numkicks: the total number of replacement
     *
     * @return depth on success, -1 otherwise
     */
    int cuckoopath_search(TableInfo* ti,
                          CuckooRecord* cuckoo_path,
                          size_t &cp_index,
                          size_t &num_kicks) {

        int depth = 0;
        while ((num_kicks < MAX_CUCKOO_COUNT) &&
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
                    if (is_slot_empty(ti, i, j)) {
                        curr->slots[idx] = j;
                        cp_index   = idx;
                        return depth;
                    }
                }

                /* pick the victim as the j-th item */
                j = (cheap_rand() >> 20) % SLOT_PER_BUCKET;

                uint32_t hv        = hashed_key(ti->buckets_[i].keys[j]);
                curr->slots[idx]   = j;
                curr->keys[idx]    = ti->buckets_[i].keys[j];
                next->buckets[idx] = alt_index(ti, hv, i);
            }

            num_kicks += NUM_CUCKOO_PATH;
            depth++;
        }

        DBG("%zu max cuckoo achieved, abort\n", num_kicks);
        return -1;
    }


    /**
     * @brief Move keys along the given cuckoo path
     *
     * @param cuckoo_path: cuckoo path
     * @param depth: the depth to start
     * @param idx: which cuckoo path is good
     *
     * @return the depth we stopped at, 0 means success
     */
    int cuckoopath_move(TableInfo* ti,
                        CuckooRecord* cuckoo_path,
                        size_t depth,
                        const size_t idx) {

        while (depth > 0) {

            /*
             * Move the key/value from buckets_[i1] slot[j1] to buckets_[i2] slot[j2]
             * buckets_[i1] slot[j1] will be available after move
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
            if (ti->buckets_[i1].keys[j1] != from->keys[idx]) {
                /* try again */
                return depth;
            }

            assert(is_slot_empty(ti, i2, j2));

            start_incr_counter2(ti, i1, i2);

            ti->buckets_[i2].keys[j2] = ti->buckets_[i1].keys[j1];
            ti->buckets_[i2].vals[j2] = ti->buckets_[i1].vals[j1];
            set_slot_used(ti, i2, j2);
            set_slot_empty(ti, i1, j1);

            end_incr_counter2(ti, i1, i2);

            depth--;
        }

        return depth;
    }

    bool run_cuckoo(TableInfo* ti,
                    const size_t i1,
                    const size_t i2,
                    size_t &i) {
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
            cuckoo_path = new CuckooRecord[MAX_CUCKOO_COUNT];
        }

        //memset(cuckoo_path, 0, MAX_CUCKOO_COUNT * sizeof(CuckooRecord));

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

            int depth = cuckoopath_search(ti, cuckoo_path, idx, num_kicks);
            if (depth < 0) {
                break;
            }

            int curr_depth = cuckoopath_move(ti, cuckoo_path, depth, idx);
            if (0 == curr_depth) {
                i = cuckoo_path[0].buckets[idx];
                done = true;
                break;
            }
        }

#ifdef __linux__
#else
        delete [] cuckoo_path;
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
    bool try_read_from_bucket(const TableInfo* ti,
                              const key_type &key,
                              mapped_type    &val,
                              const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            /* check if this slot is used */
            if (is_slot_empty(ti, i, j)) {
                continue;
            }

            if (key == ti->buckets_[i].keys[j]) {
                val = ti->buckets_[i].vals[j];
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
    bool try_add_to_bucket(TableInfo* ti,
                           const key_type    &key,
                           const mapped_type &val,
                           const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            if (is_slot_empty(ti, i, j)) {

                start_incr_counter(ti, i);
                ti->buckets_[i].keys[j] = key;
                ti->buckets_[i].vals[j] = val;
                set_slot_used(ti, i, j);
                end_incr_counter(ti, i);

                ti->hashitems_.fetch_add(1);
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
    bool try_del_from_bucket(TableInfo* ti,
                             const key_type &key,
                             const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            /* check if this slot is used */
            if (is_slot_empty(ti, i, j)) {
                continue;
            }

            if (ti->buckets_[i].keys[j] == key) {
                start_incr_counter(ti, i);
                set_slot_empty(ti, i, j);
                end_incr_counter(ti, i);

                ti->hashitems_.fetch_sub(1);
               return true;
           }
       }
       return false;
   }


   /* non-thread safe cuckoo hashing functions: cuckoo_find,
    * cuckoo_insert, cuckoo_delete cuckoo_expand, cuckoo_report */

   /**
    * @brief find key in bucket i1 and i2
    *
    * @param key the key to search
    * @param val the value to return
    * @param i1  1st bucket index
    * @param i2  2nd bucket index
    *
    * @return ok on success
    */
   cuckoo_status cuckoo_find(const TableInfo* ti,
                             const key_type &key,
                             mapped_type    &val,
                             const size_t i1,
                             const size_t i2) {
       bool result;

       result = try_read_from_bucket(ti, key, val, i1);
       if (!result) {
           result = try_read_from_bucket(ti, key, val, i2);
       }

       if (result) {
           return ok;
       } else {
           return failure_key_not_found;
       }
   }

   /**
    * @brief insert (key, val) to bucket i1 or i2
    *        the lock should be acquired by the caller
    *
    * @param key the key to insert
    * @param val the value associated with this key
    * @param val the value to return
    * @param i1  1st bucket index
    * @param i2  2nd bucket index
    *
    * @return ok on success
    */
   cuckoo_status cuckoo_insert(TableInfo* ti,
                               const key_type    &key,
                               const mapped_type &val,
                               const size_t i1,
                               const size_t i2) {

       /*
        * try to add new key to bucket i1 first, then try bucket i2
        */
       if (try_add_to_bucket(ti, key, val, i1)) {
           return ok;
       }

       if (try_add_to_bucket(ti, key, val, i2)) {
           return ok;
       }

       /*
        * we are unlucky, so let's perform cuckoo hashing
        */
       size_t i = 0;

       if (run_cuckoo(ti, i1, i2, i)) {
           if (try_add_to_bucket(ti, key, val, i)) {
               return ok;
           }
       }

       DBG("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
           ti->hashpower_, ti->hashitems_.load(), load_factor());

       return failure_table_full;
   }

   /**
    * @brief delete key from bucket i1 or i2
    *        the lock should be acquired by the caller
    *
    * @param key the key to delete
    * @param i1  1st bucket index
    * @param i2  2nd bucket index
    *
    * @return ok on success
    */
   cuckoo_status cuckoo_delete(TableInfo* ti,
                               const key_type &key,
                               const size_t i1,
                               const size_t i2) {
       if (try_del_from_bucket(ti, key, i1)) {
           return ok;
       }

       if (try_del_from_bucket(ti, key, i2)) {
           return ok;
       }

       return failure_key_not_found;
   }


   /**
    * @brief update the value of key to val in bucket i1 or i2
    *        the lock should be acquired by the caller
    *
    * @param key the key to update
    * @param val the new value
    * @param i1  1st bucket index
    * @param i2  2nd bucket index
    *
    * @return ok on success
    */
   cuckoo_status cuckoo_update(TableInfo* ti,
                               const key_type    &key,
                               const mapped_type &val,
                               const size_t i1,
                               const size_t i2) {
       cuckoo_status st;

       st = cuckoo_delete(ti, key, i1, i2);
       if (ok != st) {
           return st;
       }

       st = cuckoo_insert(ti, key, val, i1, i2);

       return st;
   }

   /**
    * @brief clean a given number of buckets
    *        the lock should be acquired by the caller
    *
    * @return
    */
   cuckoo_status cuckoo_clean(TableInfo* ti, const size_t size) {
       for (size_t ii = 0; ii < size; ii++) {
           size_t i = cleaned_buckets_;
           for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
               if (0 == get_slot_flag(ti, i, j)) {
                   continue;
               }
               uint32_t hv = hashed_key(ti->buckets_[i].keys[j]);
               size_t   i1 = index_hash(ti, hv);
               size_t   i2 = alt_index(ti, hv, i1);

               if ((i != i1) && (i != i2)) {
                   //DBG("delete key %u , i=%zu i1=%zu i2=%zu\n", slot_key(h, i, j), i, i1, i2);
                   set_slot_empty(ti, i, j);
               }
           }
           cleaned_buckets_++;
           if (cleaned_buckets_ == hashsize(ti->hashpower_)) {
               expanding_.store(false);
               DBG("table clean done, cleaned_buckets = %zu\n", cleaned_buckets_);
               return ok;
           }
       }
       //DBG("_cuckoo_clean: cleaned_buckets = %zu\n", h->cleaned_buckets);
   }

   cuckoo_status cuckoo_init(const size_t hashtable_init) {
       TableInfo* new_table_info(new TableInfo);
       try {
           new_table_info->hashpower_  = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
           new_table_info->tablesize_  = sizeof(Bucket) * hashsize(new_table_info->hashpower_);
           new_table_info->buckets_    = std::move(std::unique_ptr<Bucket[]>(new Bucket[hashsize(new_table_info->hashpower_)]));
           new_table_info->counters_   = std::move(std::unique_ptr<uint32_t[]>(new uint32_t[kNumCounters]));
       } catch (std::bad_alloc&) {
           delete new_table_info;
           return failure;
       }

       pthread_mutex_init(&lock_, NULL);

       table_info.store(new_table_info);
       cuckoo_clear(table_info.load());

       return ok;
   }


   cuckoo_status cuckoo_clear(TableInfo* ti) {
       for (size_t i = 0; i < hashsize(ti->hashpower_); i++) {
           ti->buckets_[i].flag = 0;
       }
       memset(ti->counters_.get(), 0, kNumCounters * sizeof(uint32_t));
       ti->hashitems_ = 0;
       expanding_.store(false);

       return ok;
   }


   // Expects the lock and the expanding_ variable to already be set
   cuckoo_status cuckoo_expand() {

       TableInfo* ti = table_info.load();

       std::unique_ptr<Bucket[]> new_buckets(new Bucket[hashsize(ti->hashpower_) * 2]);
       printf("current: power %zu, tablesize %zu\n", ti->hashpower_, ti->tablesize_);

       memcpy(new_buckets.get(), ti->buckets_.get(), ti->tablesize_);
       memcpy(new_buckets.get() + hashsize(ti->hashpower_), ti->buckets_.get(), ti->tablesize_);

       ti->buckets_ = std::move(new_buckets);

       reorder_barrier();

       ti->hashpower_++;
       ti->tablesize_ <<= 1;
       cleaned_buckets_ = 0;

       return ok;
   }


   /* A simpler version of expansion, which will return a completely
    * correct hash table by the end. Expects the lock to already be
    * taken. */
   cuckoo_status cuckoo_expand_simple() {
       const TableInfo* ti = table_info.load();
       // Creates a new hash table twice the size and adds all the
       // elements from the old buckets
       cuckoohash_map<Key, T, Hash> new_map(ti->hashpower_+1);
       for (size_t i = 0; i < bucket_count(); i++) {
           for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
               if (!is_slot_empty(ti, i, j)) {
                   new_map.insert(ti->buckets_[i].keys[j], ti->buckets_[i].vals[j]);
               }
           }
       }
       // Exchanges this table_info with new_map's, keeping the
       // pointer to the old one. It then sets new_map's table_info
       // to nullptr, so that it doesn't get deleted when new_map
       // goes out of scope
       TableInfo* old_ti = table_info.exchange(new_map.table_info.load());
       new_map.table_info.store(nullptr);
       std::this_thread::sleep_for(std::chrono::milliseconds(100));
       delete old_ti;
       return ok;
   }

   cuckoo_status cuckoo_report() {
       TableInfo* ti = table_info.load();
       printf("total number of items %zu\n", ti->hashitems_.load());
       printf("total size %zu Bytes, or %.2f MB\n",
              ti->tablesize_, (float) ti->tablesize_ / (1 << 20));
       printf("load factor %.4f\n", load_factor());

       return ok;
   }

// Iterator functions
public:
   typedef c_iterator<key_type, mapped_type, hasher> const_iterator;
   const_iterator cbegin();
   const_iterator cend();

   typedef mut_iterator<key_type, mapped_type, hasher> iterator;
   iterator begin();
   iterator end();

   friend class c_iterator<key_type, mapped_type, hasher>;
   friend class mut_iterator<key_type, mapped_type, hasher>;

    value_type* snapshot_table();
};

/* An iterator through the table that is thread safe. For the
* duration of its existence, it takes a write lock on the table,
* thereby ensuring that no other threads can modify the table
* while the iterator is in use. It allows movement forward and
* backward through the table as well as dereferencing items in
* the table. We maintain the assertion that an iterator is either
* an end iterator (which points past the end of the table), or it
* points to a filled slot. As soon as the iterator looses its
* lock on the table, all operations will throw an exception. */
template <class K, class V, class H>
class c_iterator {
private:
   typedef std::pair<K, V> KVPair;

public:
   // Constructors/destructors/assignment

   // Takes the lock, calculates end_pos and begin_pos, and sets
   // index and slot to the beginning or end of the table, based on
   // the boolean argument
   c_iterator(cuckoohash_map<K, V, H> *hm, bool is_end) {
       hm_ = hm;
       mutex_lock(&hm_->lock_);
       has_table_lock = true;
       ti_ = hm_->table_info.load();

       set_end(end_pos.first, end_pos.second);
       set_begin(begin_pos.first, begin_pos.second);
       if (is_end) {
           index_ = end_pos.first;
           slot_ = end_pos.second;
       } else {
           index_ = begin_pos.first;
           slot_ = begin_pos.second;
       }
   }

   c_iterator(c_iterator&& it) {
       if (this == &it) {
           return;
       }
       memcpy(this, &it, sizeof(c_iterator));
       it.has_table_lock = false;
   }

   c_iterator* operator=(c_iterator&& it) {
       if (this == &it) {
           return this;
       }
       memcpy(this, &it, sizeof(c_iterator));
       it.has_table_lock = false;
       return this;
   }

   // Releases the lock on the table, thereby freeing the table,
   // but also invalidating all future operations with this
   // iterator
   void release() {
       if (has_table_lock) {
           mutex_unlock(&hm_->lock_);
           has_table_lock = false;
       }
   }

   ~c_iterator() {
       release();
   }

   // Returns true if the iterator is at end_pos
   bool is_end() {
       return (index_ == end_pos.first && slot_ == end_pos.second);
   }
   // Returns true if the iterator is at begin_pos
   bool is_begin() {
       return (index_ == begin_pos.first && slot_ == begin_pos.second);
   }

   // Pointer operations

   // We return a copy of the data in the buckets, so that any
   // modification to the returned value_type doesn't affect the
   // table itself. This is also the reason we don't have a ->
   // operator.
   KVPair operator*() {
       check_lock();
       if (is_end()) {
           throw std::out_of_range(end_dereference);
       }
       assert(!hm_->is_slot_empty(index_, slot_));
       return KVPair(ti_->buckets_[index_].keys[slot_], ti_->buckets_[index_].vals[slot_]);
   }

       // Moves forwards to the next nonempty slot. If it reaches the
    // end of the table, it becomes an end iterator.
    c_iterator* operator++() {
        check_lock();
        if (is_end()) {
            throw std::out_of_range(end_increment);
        }
        forward_filled_slot(index_, slot_);
        return this;
    }
    c_iterator* operator++(int) {
        check_lock();
        if (is_end()) {
            throw std::out_of_range(end_increment);
        }
        forward_filled_slot(index_, slot_);
        return this;
    }

    // Moves backwards to the next nonempty slot. If we aren't at
    // the beginning, then the backward_filled_slot operation should
    // not fail.
    c_iterator* operator--() {
        check_lock();
        if (is_begin()) {
            throw std::out_of_range(begin_decrement);
        }
        bool res = backward_filled_slot(index_, slot_);
        assert(res);
        return this;
    }
    c_iterator* operator--(int) {
        check_lock();
        if (is_begin()) {
            throw std::out_of_range(begin_decrement);
        }
        bool res = backward_filled_slot(index_, slot_);
        assert(res);
        return this;
    }

protected:

    /* A pointer to the associated hashmap */
    cuckoohash_map<K, V, H> *hm_;
    /* The hashmap's table info */
    typename cuckoohash_map<K, V, H>::TableInfo* ti_;

    /* Indicates whether the iterator has the table lock */
    bool has_table_lock;

    /* Indicates the bucket and slot of the end iterator, which is
     * one past the end of the table. This is determined upon
     * initializing the iterator */
    std::pair<size_t, size_t> end_pos;

    /* Indicates the bucket and slot of the first filled position
     * in the table. This is determined upon initializing the
     * iterator. If the table is empty, it points past the end of
     * the table, to the same value as end_pos */
    std::pair<size_t, size_t> begin_pos;

    /* The bucket index of the item being pointed to */
    size_t index_;

    /* The slot in the bucket of the item being pointed to */
    size_t slot_;

    // Sets the given index and slot to one past the last position
    // in the table
    void set_end(size_t& index, size_t& slot) {
        index = hm_->bucket_count();
        slot = 0;
    }

    // Sets the given pair to the position of the first element in
    // the table.
    void set_begin(size_t& index, size_t& slot) {
        if (hm_->size() == 0) {
            // Initializes begin_pos to past the end of the table
            set_end(index, slot);
        } else {
            index = slot = 0;
            // There must be a filled slot somewhere in the table
            if (hm_->is_slot_empty(ti_, index, slot)) {
                bool res = forward_filled_slot(index, slot);
                assert(res);
            }
            // index and slot now point to the first filled
            // element
        }
    }

    // Moves the given index and slot to the next available slot
    // in the forwards direction. Returns true if it successfully
    // advances, and false if it has reached the end of the table,
    // in which case it sets index and slot to end_pos
    bool forward_slot(size_t& index, size_t& slot) {
        if (slot < SLOT_PER_BUCKET-1) {
            ++slot;
            return true;
        } else if (index < hm_->bucket_count()-1) {
            ++index;
            slot = 0;
            return true;
        } else {
            set_end(index, slot);
            return false;
        }
    }
    // Moves index and slot to the next available slot in the
    // backwards direction. Returns true if it successfully
    // advances, and false if it has reached the beginning of the
    // table, setting the index and slot back to begin_pos
    bool backward_slot(size_t& index, size_t& slot) {
        if (slot > 0) {
            --slot;
            return true;
        } else if (index > 0) {
            --index;
            slot = SLOT_PER_BUCKET-1;
            return true;
        } else {
            set_begin(index, slot);
            return false;
        }
    }

    // Moves index and slot to the next filled slot.
    bool forward_filled_slot(size_t& index, size_t& slot) {
        bool res = forward_slot(index, slot);
        if (!res) {
            return false;
        }
        while (hm_->is_slot_empty(ti_, index, slot)) {
            res = forward_slot(index, slot);
            if (!res) {
                return false;
            }
        }
        return true;
    }
    // Moves index and slot to the previous filled slot.
    bool backward_filled_slot(size_t& index, size_t& slot) {
        bool res = backward_slot(index, slot);
        if (!res) {
            return false;
        }
        while (hm_->is_slot_empty(ti_, index, slot)) {
            res = backward_slot(index, slot);
            if (!res) {
                return false;
            }
        }
        return true;
    }

    static constexpr const char* unlocked_errmsg = "Iterator does not have a lock on the table";
    void check_lock() {
        if (!has_table_lock) {
            throw std::out_of_range(unlocked_errmsg);
        }
    }

    static constexpr const char* end_dereference = "Cannot dereference: iterator points past the end of the table";
    static constexpr const char* end_increment = "Cannot increment: iterator points past the end of the table";
    static constexpr const char* begin_decrement = "Cannot decrement: iterator points to the beginning of the table";

};

/* A mut_iterator is just a c_iterator with an update method */
template <class K, class V, class H>
class mut_iterator : public c_iterator<K, V, H> {
public:
    mut_iterator(cuckoohash_map<K, V, H> *hm, bool is_end)
        : c_iterator<K, V, H>(hm, is_end) {}
    mut_iterator(mut_iterator&& it)
        : c_iterator<K, V, H>(std::move(it)) {}
    mut_iterator(c_iterator<K,V,H>&& it)
        : c_iterator<K, V, H>(std::move(it)) {}

    mut_iterator* operator=(mut_iterator&& it) {
        if (this == &it) {
            return this;
        }
        memcpy(this, &it, sizeof(mut_iterator));
        it.has_table_lock = false;
        return this;
    }

    // Sets the value pointed to by the iterator. This involves
    // modifying the hash table itself, but since we have a lock
    // on the table, we are okay. Since we are only changing the
    // value in the bucket, the element will retain it's position,
    // so this is just a simple assignment.
    void set_value(const V val) {
        this->check_lock();
        if (this->is_end()) {
            throw std::out_of_range(this->end_dereference);
        }
        assert(!this->hm_->is_slot_empty(this->ti_, this->index_, this->slot_));
        this->ti_->buckets_[this->index_].vals[this->slot_] = val;
    }
};

// Implementations of the cuckoohash_map iterator functions
template <class K, class V, class H>
typename cuckoohash_map<K,V,H>::const_iterator cuckoohash_map<K,V,H>::cbegin() {
    return cuckoohash_map<K,V,H>::const_iterator(this, false);
}
template <class K, class V, class H>
typename cuckoohash_map<K,V,H>::const_iterator cuckoohash_map<K,V,H>::cend() {
    return cuckoohash_map<K,V,H>::const_iterator(this, true);
}
template <class K, class V, class H>
typename cuckoohash_map<K,V,H>::iterator cuckoohash_map<K,V,H>::begin() {
    return cuckoohash_map<K,V,H>::iterator(this, false);
}
template <class K, class V, class H>
typename cuckoohash_map<K,V,H>::iterator cuckoohash_map<K,V,H>::end() {
    return cuckoohash_map<K,V,H>::iterator(this, true);
}

// Using a const_iterator, it allocates an array and
// stores all the elements currently in the table, returning the
// array.
template <class K, class V, class H>
typename cuckoohash_map<K,V,H>::value_type* cuckoohash_map<K,V,H>::snapshot_table() {
    typename cuckoohash_map<K,V,H>::value_type* items = new typename cuckoohash_map<K,V,H>::value_type[cuckoohash_map<K,V,H>::size()];
    size_t ind = 0;
    typename cuckoohash_map<K,V,H>::const_iterator it = cuckoohash_map<K,V,H>::cbegin();
    while (!it.is_end()) {
        items[ind++] = *it;
        it++;
    }
    return items;
}

#endif
