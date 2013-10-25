#ifndef _CUCKOOHASH_MAP_H
#define _CUCKOOHASH_MAP_H
#include <functional>  // for std::hash
#include <utility>     // for std::pair

#include "cuckoohash_config.h"
#include "cuckoohash_util.h"
#include "util.h"
extern "C" {
#include "city.h"
}

// Forward declaration
template <class Key, class T, class Hash>
    class cuckoohash_map;


template <class Key, class T, class Hash>
    struct cuckoohash_map_iterator;

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


template <class Key, 
          class T,
          class Hash = std::hash<Key> >
    struct cuckoohash_map_iterator  {
public:
    typedef Key               key_type;
    typedef std::pair<Key, T> value_type;
    typedef T                 mapped_type;
    typedef Hash              hasher;

    typedef cuckoohash_map_iterator<key_type, mapped_type, hasher> iterator;

    cuckoohash_map_iterator(const cuckoohash_map<key_type, mapped_type, hasher> *h,
                            const key_type& k,
                            const mapped_type& v)
    : ht(h), key(k), val(v), is_end(false), data(value_type(k, v)) {
    }

    cuckoohash_map_iterator(const iterator& it)
    : ht(it.ht), key(it.key), val(it.val), is_end(it.is_end), data(it.data) {
    }

    cuckoohash_map_iterator(const cuckoohash_map<key_type, mapped_type, hasher> *h,
                            bool is_end)
    : ht(h), is_end(is_end) {
    }

    cuckoohash_map_iterator()
    : ht(NULL), is_end(false) {
    }

    virtual ~cuckoohash_map_iterator() { }

    // Dereferencer
    value_type& operator*()  {
        return data;
    }

    value_type* operator->()  {
        return &data;
    }

    // Arithmic, not supported yet
    iterator& operator++() {
        assert(false);
    }

    iterator& operator++(int) {
        assert(false);
    }

    // Comparison
    bool operator==(const iterator& it) const {
        if (is_end && it.is_end)
            return true;
        else
            return key == it.key;
    }

    bool operator!=(const iterator& it) const {
        if (is_end || it.is_end)
            return true;
        else
            return key != it.key;
    }

    // The actual data
    const cuckoohash_map<key_type, mapped_type, hasher> *ht;
    key_type    key;
    mapped_type val;
    bool        is_end;
    value_type  data;
 };


template <class Key, 
          class T, 
          class Hash = std::hash<Key> >
    class cuckoohash_map {
public:
    // Type definitions
    typedef Key               key_type;
    typedef std::pair<Key, T> value_type;
    typedef T                 mapped_type;
    typedef Hash              hasher;

    typedef cuckoohash_map_iterator<key_type, mapped_type, hasher> iterator;

    typedef size_t size_type;

    // Constructor and destructor
    explicit cuckoohash_map(size_t hashpower_init = HASHPOWER_DEFAULT) {
        cuckoo_init(hashpower_init);
    }

    ~cuckoohash_map() {
        pthread_mutex_destroy(&lock_);
        delete [] buckets_;
        delete [] counters_;
    }

    void clear() {
        cuckoo_clear();
    }

    // Iterator functions
    iterator begin() {
        assert(false);
        return iterator(this);
    }

    iterator end() {
        return iterator(this, true);
    }


    // Functions concerning size
    size_type size() const {
        return hashitems_;
    }

    size_type max_size() const {
        assert(false);
        return 0;
    }

    bool empty() const {
        return (hashitems_ == 0);
    }

    size_type bucket_count() const {
        return hashsize(hashpower_);
    }

    size_type max_bucket_count() const {
        assert(false);
        return 0;
    }


    // Load factor
    float load_factor() const {
        return 1.0 * hashitems_ / SLOT_PER_BUCKET / hashsize(hashpower_);
    }

    float max_load_factor() const {
        return 1.0;
    }


    // Lookup routines
    bool find(key_type& key, mapped_type& val) {
        if (size() == 0) return false;

        uint32_t hv = hashed_key(key);
        //
        // potential "false read miss" under expansion:
        // calculation of i1 and i2 may use stale hashpower
        //
        size_t   i1 = index_hash(hv);
        size_t   i2 = alt_index(hv, i1);

        uint32_t vs1, vs2, ve1, ve2;
    TryRead:
        start_read_counter2(i1, i2, vs1, vs2);

        if (((vs1 & 1) || (vs2 & 1) )) {
            goto TryRead;
        }

        cuckoo_status st = cuckoo_find(key, val, i1, i2);

        end_read_counter2(i1, i2, ve1, ve2);

        if (((vs1 != ve1) || (vs2 != ve2))) {
            goto TryRead;
        }

        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    iterator find(key_type& key) {
        if (size() == 0) return end();

        mapped_type val;

        bool done = find(key, val);

        if (done) {
            return iterator(this, key, val);
        } else {
            return end();
        }
    }

    // Insertion routines
    bool insert(key_type& key, mapped_type& val) {

        uint32_t hv = hashed_key(key);
        mutex_lock(&lock_);

        size_t   i1 = index_hash(hv);
        size_t   i2 = alt_index(hv, i1);

        cuckoo_status st;
        mapped_type oldval;

        st = cuckoo_find(key, oldval, i1, i2);
        if (ok == st) {
            mutex_unlock(&lock_);
            return false; // failure_key_duplicated;
        }

        st = cuckoo_insert(key, val, i1, i2);

        if (expanding_) {
            /*
             * still some work to do before releasing the lock
             */
            cuckoo_clean(DEFAULT_BULK_CLEAN);
        }

        mutex_unlock(&lock_);

        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    // Deletion routines
    bool erase(key_type& key) {
        uint32_t hv = hashed_key(key);

        mutex_lock(&lock_);

        size_t   i1 = index_hash(hv);
        size_t   i2 = alt_index(hv, i1);

        cuckoo_status st = cuckoo_delete(key, i1, i2);

        mutex_unlock(&lock_);

        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    /* // None standard functions: */

    bool expand() {

        Bucket* old_buckets = NULL;

        mutex_lock(&lock_);
        
        old_buckets = buckets_;
        cuckoo_status st = cuckoo_expand();

        mutex_unlock(&lock_);

        if (st == ok) {
            delete [] old_buckets;
            return true;
        } else {
            return false;
        }
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

    /* key size in bytes */
    static const size_t kKeySize   = sizeof(key_type);

    /* value size in bytes */
    static const size_t kValueSize = sizeof(mapped_type);

    /* size of a bucket in bytes */
    static const size_t kBucketSize = sizeof(Bucket);

    /* size of counter array */
    static const size_t kNumCounters = 1 << 13;

    /* size of the table in bytes */
    size_t tablesize_;

    /* number of items inserted */
    size_t hashitems_;

    /* 2**hashpower is the number of buckets */
    volatile size_t hashpower_;

    /* pointer to the array of buckets */
    Bucket* buckets_;

    /* the array of version counters 
     * we keep keyver_count = 8192
     */
    uint32_t* counters_;

    /* the mutex to serialize insert, delete, expand */
    pthread_mutex_t lock_;

    /* denoting if the table is doing expanding */
    bool expanding_;

    /* number of buckets has been cleaned */
    size_t cleaned_buckets_;

    uint32_t hashed_key(const key_type &key) {
        return hasher()(key);
        //return CityHash32((const char*) &key, kKeySize);
    }

    /**
     * @brief Compute the index of the first bucket
     *
     * @param hv 32-bit hash value of the key
     *
     * @return The first bucket
     */
    size_t index_hash(const uint32_t hv) const {
        return hv & hashmask(hashpower_);
    }


    /**
     * @brief Compute the index of the second bucket
     *
     * @param hv 32-bit hash value of the key
     * @param index The index of the first bucket
     *
     * @return  The second bucket
     */
    size_t alt_index(const uint32_t hv,
                     const size_t index) const {
        uint32_t tag = (hv >> 24) + 1; // ensure tag is nonzero for the multiply
        /* 0x5bd1e995 is the hash constant from MurmurHash2 */
        return (index ^ (tag * 0x5bd1e995)) & hashmask(hashpower_);
    }


    bool get_slot_flag(const size_t i, const size_t j) const {
        assert(i < hashsize(hashpower_));
        assert(j < SLOT_PER_BUCKET);
        return get_bit(&buckets_[i].flag, j);
    }

    void set_slot_used(const size_t i, const size_t j) {
        assert(i < hashsize(hashpower_));
        assert(j < SLOT_PER_BUCKET);
        set_bit(&buckets_[i].flag, j, 1);
    }

    void set_slot_empty(const size_t i, const size_t j) {
        assert(i < hashsize(hashpower_));
        assert(j < SLOT_PER_BUCKET);
        set_bit(&buckets_[i].flag, j, 0);
    }
    
    bool is_slot_empty(const size_t i,const size_t j) {
        if (0 == get_slot_flag(i, j)) {
            return true;
        }

        /*
         * even it shows "non-empty", it could be a false alert during expansion
         */
        if (expanding_ && i >= cleaned_buckets_) {
            // when we are expanding
            // we could leave keys in their old but wrong buckets
            uint32_t hv = hashed_key(buckets_[i].keys[j]);
            size_t   i1 = index_hash(hv);
            size_t   i2 = alt_index(hv, i1);

            if ((i != i1) && (i != i2)) {
                set_slot_empty(i, j);
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
    int cuckoopath_search(CuckooRecord* cuckoo_path,
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
                    if (is_slot_empty(i, j)) {
                        curr->slots[idx] = j;
                        cp_index   = idx;
                        return depth;
                    }
                }

                /* pick the victim as the j-th item */
                j = (cheap_rand() >> 20) % SLOT_PER_BUCKET;

                uint32_t hv        = hashed_key(buckets_[i].keys[j]);
                curr->slots[idx]   = j;
                curr->keys[idx]    = buckets_[i].keys[j];
                next->buckets[idx] = alt_index(hv, i);
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
    int cuckoopath_move(CuckooRecord* cuckoo_path,
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
            if (buckets_[i1].keys[j1] != from->keys[idx]) {
                /* try again */
                return depth;
            }

            assert(is_slot_empty(i2, j2));

            start_incr_counter2(i1, i2);

            buckets_[i2].keys[j2] = buckets_[i1].keys[j1];
            buckets_[i2].vals[j2] = buckets_[i1].vals[j1];
            set_slot_used(i2, j2);
            set_slot_empty(i1, j1);

            end_incr_counter2(i1, i2);

            depth--;
        }

        return depth;
    }


    bool run_cuckoo(const size_t i1,
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

            int depth = cuckoopath_search(cuckoo_path, idx, num_kicks);
            if (depth < 0) {
                break;
            }

            int curr_depth = cuckoopath_move(cuckoo_path, depth, idx);
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
    bool try_read_from_bucket(const key_type &key,
                              mapped_type    &val,
                              const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            /* check if this slot is used */
            if (is_slot_empty(i, j)) {
                continue;
            }

            if (key == buckets_[i].keys[j]) {
                val = buckets_[i].vals[j];
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
    bool try_add_to_bucket(const key_type    &key,
                           const mapped_type &val,
                           const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            if (is_slot_empty(i, j)) {

                start_incr_counter(i);
                buckets_[i].keys[j] = key;
                buckets_[i].vals[j] = val;
                set_slot_used(i, j);
                end_incr_counter(i);

                hashitems_++;
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
    bool try_del_from_bucket(const key_type &key,
                             const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            /* check if this slot is used */
            if (is_slot_empty(i, j)) {
                continue;
            }

            if (buckets_[i].keys[j] == key) {
                start_incr_counter(i);
                set_slot_empty(i, j);
                end_incr_counter(i);

                hashitems_--;
                return true;
            }
        }
        return false;
    }


    //
    // non-thread safe cuckoo hashing functions:
    // cuckoo_find, cuckoo_insert, cuckoo_delete
    // cuckoo_expand, cuckoo_report
    //

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
    cuckoo_status cuckoo_find(const key_type &key,
                              mapped_type    &val,
                              const size_t i1,
                              const size_t i2) {
        bool result;

        result = try_read_from_bucket(key, val, i1);
        if (!result) {
            result = try_read_from_bucket(key, val, i2);
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
    cuckoo_status cuckoo_insert(const key_type    &key,
                                const mapped_type &val,
                                const size_t i1,
                                const size_t i2) {

        /*
         * try to add new key to bucket i1 first, then try bucket i2
         */
        if (try_add_to_bucket(key, val, i1)) {
            return ok;
        }

        if (try_add_to_bucket(key, val, i2)) {
            return ok;
        }

        /*
         * we are unlucky, so let's perform cuckoo hashing
         */
        size_t i = 0;

        if (run_cuckoo(i1, i2, i)) {
            if (try_add_to_bucket(key, val, i)) {
                return ok;
            }
        }

        DBG("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
            hashpower_, hashitems_, load_factor());

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
    cuckoo_status cuckoo_delete(const key_type &key,
                                const size_t i1,
                                const size_t i2) {
        if (try_del_from_bucket(key, i1)) {
            return ok;
        }

        if (try_del_from_bucket(key, i2)) {
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
    cuckoo_status cuckoo_update(const key_type    &key,
                                const mapped_type &val,
                                const size_t i1,
                                const size_t i2) {
        cuckoo_status st;

        st = cuckoo_delete(key, i1, i2);
        if (ok != st) {
            return st;
        }

        st = cuckoo_insert(key, val, i1, i2);

        return st;
    }

    /**
     * @brief clean a given number of buckets
     *        the lock should be acquired by the caller
     *
     * @return
     */
    cuckoo_status cuckoo_clean(const size_t size) {
        for (size_t ii = 0; ii < size; ii++) {
            size_t i = cleaned_buckets_;
            for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
                if (0 == get_slot_flag(i, j)) {
                    continue;
                }
                uint32_t hv = hashed_key(buckets_[i].keys[j]);
                size_t   i1 = index_hash(hv);
                size_t   i2 = alt_index(hv, i1);

                if ((i != i1) && (i != i2)) {
                    //DBG("delete key %u , i=%zu i1=%zu i2=%zu\n", slot_key(h, i, j), i, i1, i2);
                    set_slot_empty(i, j);
                }
            }
            cleaned_buckets_++;
            if (cleaned_buckets_ == hashsize(hashpower_)) {
                expanding_ = false;
                DBG("table clean done, cleaned_buckets = %zu\n", cleaned_buckets_);
                return ok;
            }
        }
        //DBG("_cuckoo_clean: cleaned_buckets = %zu\n", h->cleaned_buckets);
    }

    cuckoo_status cuckoo_init(const size_t hashtable_init) {
        hashpower_  = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
        tablesize_  = sizeof(Bucket) * hashsize(hashpower_);
        buckets_    = new Bucket[hashsize(hashpower_)];
        counters_   = new uint32_t[kNumCounters];

        pthread_mutex_init(&lock_, NULL);

        cuckoo_clear();

        return ok;
    }


    cuckoo_status cuckoo_clear() {
        for (size_t i = 0; i < hashsize(hashpower_); i++) {
            buckets_[i].flag = 0;
        }
        memset(counters_, 0, kNumCounters * sizeof(uint32_t));
        hashitems_  = 0;
        expanding_  = false;

        return ok;
    }


    cuckoo_status cuckoo_expand() {
        
        if (expanding_) {
            //DBG("expansion is on-going\n", NULL);
            return failure_under_expansion;
        }

        expanding_ = true;

        Bucket* new_buckets = new Bucket[hashsize(hashpower_) * 2];
        printf("current: power %zu, tablesize %zu\n", hashpower_, tablesize_);

        memcpy(new_buckets, buckets_, tablesize_);
        memcpy(new_buckets + hashsize(hashpower_), buckets_, tablesize_);

        buckets_ = new_buckets;

        reorder_barrier();

        hashpower_++;
        tablesize_ <<= 1;
        cleaned_buckets_ = 0;

        return ok;
    }

    cuckoo_status cuckoo_report() {
        printf("total number of items %zu\n", hashitems_);
        printf("total size %zu Bytes, or %.2f MB\n", 
               tablesize_, (float) tablesize_ / (1 << 20));
        printf("load factor %.4f\n", load_factor());

        return ok;
    }

};

#endif
