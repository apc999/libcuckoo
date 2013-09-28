#ifndef _CUCKOOHASH_MAP_HH
#define _CUCKOOHASH_MAP_HH
#include <functional>  // for std::hash
#include <utility>     // for std::pair
#include <stdexcept>
#include <cstring>
#include <cassert>

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

public:
    /* An iterator through the table that is thread safe. For the
     * duration of its existence, it takes a write lock on the table,
     * thereby ensuring that no other threads can modify the table
     * while the iterator is in use. It allows movement forward and
     * backward through the table as well as dereferencing items in
     * the table. We maintain the assertion that an iterator is either
     * an end iterator (which points past the end of the table), or it
     * points to a filled slot. As soon as the iterator looses it's
     * lock on the table, all operations will throw an exception. */
    class threadsafe_iterator {
    public:

        // Returns true if the iterator is at end_pos
        bool is_end() {
            return (index_ == end_pos.first && slot_ == end_pos.second);
        }

        // Returns true if the iterator is at begin_pos
        bool is_begin() {
            return (index_ == begin_pos.first && slot_ == begin_pos.second);
        }

        // Takes the lock, calculates end_pos and begin_pos, and sets
        // index and slot to the beginning or end of the table, based
        // on the boolean argument
        threadsafe_iterator(cuckoohash_map<Key, T, Hash> *hm, pthread_mutex_t *table_lock, Bucket *table_buckets, bool is_end) {
            hm_ = hm;
            table_lock_ = table_lock;
            table_buckets_ = table_buckets;
            mutex_lock(table_lock_);
            has_table_lock = true;
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

        threadsafe_iterator(threadsafe_iterator&& it) {
            if (this == &it) {
                return;
            }
            memcpy(this, &it, sizeof(threadsafe_iterator));
            it.has_table_lock = false;
        }

        threadsafe_iterator* operator=(threadsafe_iterator&& it) {
            if (this == &it) {
                return this;
            }
            memcpy(this, &it, sizeof(threadsafe_iterator));
            it.has_table_lock = false;
            return this;
        }

        // Releases the lock on the table, thereby freeing the table,
        // but also invalidating all future operations with this
        // iterator
        void release() {
            if (has_table_lock) {
                mutex_unlock(table_lock_);
                has_table_lock = false;
            }
        }

        ~threadsafe_iterator() {
            release();
        }

        // We return a copy of the data in the buckets, so that any
        // modification to the returned value_type doesn't affect the
        // table itself. This is also the reason we don't have a ->
        // operator.
        value_type operator*() {
            check_lock();
            if (is_end()) {
                throw std::runtime_error(end_dereference);
            }
            assert(!hm_->is_slot_empty(index_, slot_));
            return value_type(table_buckets_[index_].keys[slot_], table_buckets_[index_].vals[slot_]);
        }

        // Sets the value pointed to by the iterator. This involves
        // modifying the hash table itself, but since we have a lock
        // on the table, we are okay. Since we are only changing the
        // value in the bucket, the element will retain it's position,
        // so this is just a simple assignment.
        void set_value(const mapped_type val) {
            check_lock();
            if (is_end()) {
                throw std::runtime_error(end_dereference);
            }
            assert(!hm_->is_slot_empty(index_, slot_));
            table_buckets_[index_].vals[slot_] = val;
        }

        // Moves forwards to the next nonempty slot. If it reaches the
        // end of the table, it becomes an end iterator.
        threadsafe_iterator* operator++() {
            check_lock();
            if (is_end()) {
                throw std::runtime_error(end_increment);
            }
            forward_filled_slot(index_, slot_);
            return this;
        }
        threadsafe_iterator* operator++(int) {
            check_lock();
            if (is_end()) {
                throw std::runtime_error(end_increment);
            }
            forward_filled_slot(index_, slot_);
            return this;
        }

        // Moves backwards to the next nonempty slot. If we aren't at
        // the beginning, then the backward_filled_slot operation should
        // not fail.
        threadsafe_iterator* operator--() {
            check_lock();
            if (is_begin()) {
                throw std::runtime_error(begin_decrement);
            }
            bool res = backward_filled_slot(index_, slot_);
            assert(res);
            return this;
        }
        threadsafe_iterator* operator--(int) {
            check_lock();
            if (is_begin()) {
                throw std::runtime_error(begin_decrement);
            }
            bool res = backward_filled_slot(index_, slot_);
            assert(res);
            return this;
        }


    private:

        /* A pointer to the associated hashmap */
        cuckoohash_map<Key, T, Hash> *hm_;

        /* A pointer to the hashmap's lock */
        pthread_mutex_t *table_lock_;

        /* The hashmap's buckets */
        Bucket *table_buckets_;

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
                if (hm_->is_slot_empty(index, slot)) {
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
            while (hm_->is_slot_empty(index, slot)) {
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
            while (hm_->is_slot_empty(index, slot)) {
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
                throw std::runtime_error(unlocked_errmsg);
            }
        }

        static constexpr const char* end_dereference = "Cannot dereference: iterator points past the end of the table";
        static constexpr const char* end_increment = "Cannot increment: iterator points past the end of the table";
        static constexpr const char* begin_decrement = "Cannot decrement: iterator points to the beginning of the table";
    };

    // Returns a threadsafe_iterator to the beginning of the table,
    // locking the table in the process
    threadsafe_iterator threadsafe_begin() {
        return threadsafe_iterator(this, &lock_, buckets_, false);
    }

    // Returns a threadsafe_iterator to the end of the table,
    // locking the table in the process
    threadsafe_iterator threadsafe_end() {
        return threadsafe_iterator(this, &lock_, buckets_, true);
    }

};

#endif
