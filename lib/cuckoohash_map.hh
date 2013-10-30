#ifndef _CUCKOOHASH_MAP_HH
#define _CUCKOOHASH_MAP_HH
#include <functional>  // for std::hash
#include <utility>     // for std::pair
#include <stdexcept>
#include <cstring>
#include <cassert>
#include <memory>
#include <atomic>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>

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
        if (cuckoo_init(hashpower_init) != ok) {
            throw std::runtime_error("Initialization failure");
        }
    }

    ~cuckoohash_map() {
        TableInfo* ti = table_info.load();
        if (ti != nullptr) {
            delete ti;
        }
        for (auto it = old_table_infos.begin(); it != old_table_infos.end(); it++) {
            delete *it;
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

    size_type hashpower() const {
        return table_info.load()->hashpower_;
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

    // Lookup routines. For now, find locks the bucket indicies it
    // wants to check, so it contends with writes.
    bool find(const key_type& key, mapped_type& val) {
        if (size() == 0) return false;
        cuckoo_status st = cuckoo_find(key, val);
        return (st == ok);
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
        cuckoo_status st;
        mapped_type oldval;
        st = cuckoo_find(key, oldval);
        if (ok == st) {
            return false; // failure_key_duplicated;
        }

        st = cuckoo_insert(key, val);
        // If the insert failed, it tries again
        while (st != ok) {
            // cuckoo_insert could have either failed with
            // failure_under_expansion or failure_table_full. In the
            // first case, this means the insert operated on an old
            // version of the table, so we just re-snapshot and try
            // again. If it's failure_table_full, we try expanding
            // first.
            if (st == failure_table_full) {
                if (cuckoo_expand_simple() == failure_under_expansion) {
                    DBG("expansion is on-going\n", NULL);
                }
            }
            st = cuckoo_insert(key, val);
        }

        // if (expanding_.load()) {
        //     /*
        //      * still some work to do before releasing the lock
        //      */
        //     cuckoo_clean(ti, DEFAULT_BULK_CLEAN);
        // }

        return true;
    }

    // Deletion routines
    bool erase(const key_type& key) {
        cuckoo_status st = cuckoo_delete(key);
        return (st == ok);
    }

    // None standard functions:

    bool expand() {
        return (cuckoo_expand_simple() == ok);
    }

    void report() {
        cuckoo_report();
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
        size_t hashpower_;

        /* unique pointer to the array of buckets */
        std::unique_ptr<Bucket[]> buckets_;

        /* the unique_ptr array of mutexes. we keep kNumLocks locks */
        std::unique_ptr<std::mutex[]> locks_;
    };
    std::atomic<TableInfo*> table_info;
    // Holds pointers to old TableInfos that were replaced by a larger
    // one upon expansion. This keeps the memory alive for any
    // leftover operations
    std::vector<TableInfo*> old_table_infos;

    /* Operations involving the locks_ array */

    // Locks the lock at the given bucket index
    inline void lock(const TableInfo* ti, const size_t i) {
        ti->locks_[lock_ind(i)].lock();
    }
    // Unlocks the lock at the given bucket index
    inline void unlock(const TableInfo* ti, const size_t i) {
        ti->locks_[lock_ind(i)].unlock();
    }

    // Locks the locks at two bucket indexes, always locking the
    // earlier index first to avoid deadlock
    inline void lock_two(const TableInfo* ti, const size_t i1, const size_t i2) {
        const size_t lock_i1 = lock_ind(i1);
        const size_t lock_i2 = lock_ind(i2);
        if (lock_i1 < lock_i2) {
            ti->locks_[lock_i1].lock();
            ti->locks_[lock_i2].lock();
        } else if (lock_i2 < lock_i1) {
            ti->locks_[lock_i2].lock();
            ti->locks_[lock_i1].lock();
        } else {
            // They're the same, so just lock one
            ti->locks_[lock_i1].lock();
        }
    }
    // Unlocks the locks at two bucket indexes
    inline void unlock_two(const TableInfo* ti, const size_t i1, const size_t i2) {
        ti->locks_[lock_ind(i1)].unlock();
        if (i1 != i2) {
            ti->locks_[lock_ind(i2)].unlock();
        }
    }

    /* This function snapshots the table and locks the indicies for
     * the given hash values, storing the snapshot and table indicies
     * in reference variables. Since the positions of the bucket locks
     * depends on the number of buckets in the table, the table
     * snapshot needs to be grabbed first. However, after loading a
     * snapshot and grabbing the locks, an expansion could run and
     * create a new version of the table, leaving the old one for
     * deletion during the next expansion. In this case, we want to
     * check that the table is the same as the latest one, and if not
     * reload it and acquire the new bucket locks. After acquiring the
     * bucket locks, there is no chance that an expansion could occur
     * for the rest of the operation, since expansions need to lock
     * the whole table. There is a chance that two expansions occur
     * between the table load and bucket locking, but that's pretty
     * unlikely. */
    inline void snapshot_and_lock_two(const uint32_t hv, TableInfo*& ti, size_t& i1, size_t& i2) {
    TryAcquire:
        ti = table_info.load();
        i1 = index_hash(ti, hv);
        i2 = alt_index(ti, hv, i1);
        lock_two(ti, i1, i2);
        if (ti != table_info.load()) {
            unlock_two(ti, i1, i2);
            goto TryAcquire;
        }
    }

    // Similar to snapshot_and_lock_two, except it takes all the locks
    // and returns nullptr if the initial table_info load doesn't the
    // later one, rather than retrying
    inline TableInfo* snapshot_and_lock_all() {
        TableInfo* ti = table_info.load();
        for (size_t i = 0; i < kNumLocks; i++) {
            ti->locks_[i].lock();
        }
        if (ti != table_info.load()) {
            unlock_all(ti);
            return nullptr;
        }
        return ti;
    }

    // Unlocks all the locks
    inline void unlock_all(TableInfo* ti) {
        for (size_t i = 0; i < kNumLocks; i++) {
            ti->locks_[i].unlock();
        }
    }

    /* key size in bytes */
    static const size_t kKeySize   = sizeof(key_type);

    /* value size in bytes */
    static const size_t kValueSize = sizeof(mapped_type);

    /* size of a bucket in bytes */
    static const size_t kBucketSize = sizeof(Bucket);

    /* size of locks array */
    static const size_t kNumLocks = 1 << 13;

    /* denoting if the table is doing expanding */
    std::atomic<bool> expanding_;

    /* number of buckets has been cleaned */
    size_t cleaned_buckets_;

    // Converts an index into buckets_ to an index into locks_
    static inline size_t lock_ind(const size_t bucket_ind) {
        return bucket_ind & (kNumLocks - 1);
    }

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


    static bool get_slot_flag(const TableInfo* ti, const size_t i, const size_t j) {
        assert(i < hashsize(ti->hashpower_));
        assert(j < SLOT_PER_BUCKET);
        return get_bit(&(ti->buckets_[i].flag), j);
    }

    static void set_slot_used(const TableInfo* ti, const size_t i, const size_t j) {
        assert(i < hashsize(ti->hashpower_));
        assert(j < SLOT_PER_BUCKET);
        set_bit(&(ti->buckets_[i].flag), j, 1);
    }

    static void set_slot_empty(const TableInfo* ti, const size_t i, const size_t j) {
        assert(i < hashsize(ti->hashpower_));
        assert(j < SLOT_PER_BUCKET);
        set_bit(&(ti->buckets_[i].flag), j, 0);
    }

    static bool is_slot_empty(const TableInfo* ti, const size_t i,const size_t j) {
        return (0 == get_slot_flag(ti, i, j));
    }

    // Holds the unpacked form of a cuckoo path item
    typedef struct  {
        size_t   bucket;
        size_t   slot;
        key_type key;
    }  CuckooRecord; //__attribute__((__packed__)) CuckooRecord;

    // b_slot holds the information for a BFS path through the table
    struct b_slot {
        // The bucket of the last item in the path
        size_t bucket;
        // A compressed representation of the slots for each of the
        // buckets in the path. As long as
        // SLOT_PER_BUCKET^MAX_BFS_DEPTH < 2^sizeof(size_t)-1, the
        // pathcode shouldn't overflow
        size_t pathcode;
        // The position in the cuckoo_path that this slot occupies,
        // 0-indexed. A value of -1 indicates failure in the
        // slot_search function
        int depth;
        b_slot() {}
        b_slot(const size_t b, const size_t p, const int d)
            : bucket(b), pathcode(p), depth(d) {}
    } __attribute__((__packed__));

    // Queue used for BFS cuckoo hashing
    class b_queue {
        // The queue can hold MAX_CUCKOO_COUNT elements
        b_slot slots[MAX_CUCKOO_COUNT+1];
        size_t first;
        size_t last;

    public:
        b_queue() : first(0), last(0) {}


        void enqueue(b_slot x) {
            slots[last] = x;
            last = (last == MAX_CUCKOO_COUNT) ? 0 : last+1;
            assert(last != first);
        }

        b_slot dequeue() {
            assert(first != last);
            b_slot& x = slots[first];
            first = (first == MAX_CUCKOO_COUNT) ? 0 : first+1;
            return x;
        }

        bool not_full() {
            const size_t next = (last == MAX_CUCKOO_COUNT) ? 0 : last+1;
            return next != first;
        }
    } __attribute__((__packed__));

    // Searches for a cuckoo path using BFS. It starts with the i1 and
    // i2 buckets, and adds each non-empty slot of the bucket to the
    // queue until it finds an empty slot or the queue runs out of
    // space
    b_slot slot_search(const TableInfo* ti,
                       size_t i1,
                       size_t i2) {
        b_queue q;
        // The initial pathcode informs cuckoopath_search which bucket
        // the path starts on
        q.enqueue(b_slot(i1, 0, 0));
        q.enqueue(b_slot(i2, 1, 0));
        while (q.not_full()) {
            b_slot x = q.dequeue();
            // Picks a random slot to start from
            const size_t start = (cheap_rand() >> 20) % SLOT_PER_BUCKET;
            const size_t old_pathcode = x.pathcode;
            // We lock the bucket so that no changes occur while
            // iterating
            lock(ti, x.bucket);
            for (size_t i = 0; i < SLOT_PER_BUCKET && q.not_full(); i++) {
                size_t slot = (i+start) % SLOT_PER_BUCKET;
                // We add the current slot to x's pathcode. We store
                // all of the slots in pathcode by treating it as a
                // number with radix SLOT_PER_BUCKET, shifting the
                // existing path over one and adding the current slot
                x.pathcode = old_pathcode * SLOT_PER_BUCKET + slot;
                assert(slot == x.pathcode % SLOT_PER_BUCKET);
                if (is_slot_empty(ti, x.bucket, slot)) {
                    // This slot is empty, so we are done
                    unlock(ti, x.bucket);
                    return x;
                } else if (x.depth == MAX_BFS_DEPTH) {
                    // We can't extend this cuckoo path because it is
                    // already at the maximum depth
                    continue;
                } else {
                    // Extend the cuckoo path with the current slot,
                    // storing the alterate bucket of the key at the
                    // current slot
                    uint32_t hv = hashed_key(ti->buckets_[x.bucket].keys[slot]);
                    q.enqueue(b_slot(alt_index(ti, hv, x.bucket), x.pathcode, x.depth+1));
                }
            }
            unlock(ti, x.bucket);
        }
        // We didn't find a short-enough cuckoo path, so the queue ran
        // out of space. Return a failure value
        return b_slot(0, 0, -1);
    }

    /**
     * @brief Find a cuckoo path connecting to an empty slot. It will
     * take and release locks on specific buckets, so the data can
     * change between a run of this function and cuckoopath_move. Thus
     * cuckoopath_move checks that the data matches the cuckoo_path
     * before changing it.
     *
     * @param cuckoo_path: cuckoo path
     *
     * @return depth on success, -1 otherwise
     */
    int cuckoopath_search(const TableInfo* ti,
                          CuckooRecord* cuckoo_path,
                          size_t i1,
                          size_t i2) {
        b_slot x = slot_search(ti, i1, i2);
        if (x.depth == -1) {
            DBG("max cuckoo achieved, abort\n", NULL);
            return -1;
        }
        // Fill in the cuckoo_path slots from the end to the beginning
        for (int i = x.depth; i >= 0; i--) {
            cuckoo_path[i].slot = x.pathcode % SLOT_PER_BUCKET;
            x.pathcode /= SLOT_PER_BUCKET;
        }
        // Fill in the cuckoo_path buckets and keys from the beginning
        // to the end, using the final pathcode to figure out which
        // bucket the path starts on. It is possible that data is
        // modified between slot_search and the computation of the
        // cuckoo path, leading to an invalid cuckoo_path when
        // computing the buckets. cuckoopath_move will verify the
        // correctness of cuckoo_path as it does the swaps, so it
        // doesn't matter here.
        CuckooRecord *curr = cuckoo_path;
        if (x.pathcode == 0) {
            curr->bucket = i1;
            curr->key = ti->buckets_[i1].keys[curr->slot];
        } else {
            assert(x.pathcode == 1);
            curr->bucket = i2;
            curr->key = ti->buckets_[i2].keys[curr->slot];
        }
        for (int i = 1; i <= x.depth; i++) {
            CuckooRecord *prev = curr++;
            uint32_t prevhv = hashed_key(prev->key);
            assert(prev->bucket == index_hash(ti, prevhv) || prev->bucket == alt_index(ti, prevhv, index_hash(ti, prevhv)));
            // We get the bucket that this slot is on by computing the
            // alternate index of the previous bucket
            curr->bucket = alt_index(ti, hashed_key(prev->key), prev->bucket);
            curr->key = ti->buckets_[curr->bucket].keys[curr->slot];
        }
        return x.depth;
    }


    /**
     * @brief Move keys along the given cuckoo path. Before the start
     * of this function, the two insert-locked buckets were unlocked
     * in run_cuckoo. At the end of the function, if the function
     * returns 0 (success), then the last bucket it looks at (which is
     * either i1 or i2 in run_cuckoo) remains locked. If the function
     * is unsuccessful, then both insert-locked buckets will be
     * unlocked.
     *
     * @param cuckoo_path: cuckoo path
     * @param depth: the index of the last item in the cuckoo path
     *
     * @return true on success, false on failure
     */
    bool cuckoopath_move(TableInfo* ti,
                         CuckooRecord* cuckoo_path,
                         size_t depth) {
        if (depth == 0) {
            // There is a chance that depth == 0, when
            // try_add_to_bucket sees one of the insert-locked buckets
            // as full and cuckoopath_search finds one empty. In this
            // case, we lock the bucket. If it's not empty anymore, we
            // unlock the bucket and return false, so that run_cuckoo
            // will try again. Otherwise, the bucket is empty and
            // insertable, so we hold the lock and return true
            size_t bucket = cuckoo_path[0].bucket;
            lock(ti, bucket);
            if (is_slot_empty(ti, bucket, cuckoo_path[0].slot)) {
                return true;
            } else {
                unlock(ti, bucket);
                return false;
            }
        }

        while (depth > 0) {

            /*
             * Move the key/value from buckets_[i1] slot[j1] to buckets_[i2] slot[j2]
             * buckets_[i1] slot[j1] will be available after move
             */
            CuckooRecord *from = cuckoo_path + depth - 1;
            CuckooRecord *to   = cuckoo_path + depth;
            size_t i1 = from->bucket;
            size_t j1 = from->slot;
            size_t i2 = to->bucket;
            size_t j2 = to->slot;

            lock_two(ti, i1, i2);

            /*
             * We plan to kick out j1, but let's check if it is still
             * there; there's a small chance we've gotten scooped by a
             * later cuckoo. If that happened, just... try again. Also
             * the slot we are filling in may have already been filled
             * in by another thread, or the slot we are moving from
             * may be empty, both of which invalidate the swap.
             */
            if ((ti->buckets_[i1].keys[j1] != from->key) ||
                !is_slot_empty(ti, i2, j2) || is_slot_empty(ti, i1, j1)) {
                /* run_cuckoo will try again */
                unlock_two(ti, i1, i2);
                return false;
            }

            ti->buckets_[i2].keys[j2] = ti->buckets_[i1].keys[j1];
            ti->buckets_[i2].vals[j2] = ti->buckets_[i1].vals[j1];
            set_slot_used(ti, i2, j2);
            set_slot_empty(ti, i1, j1);

            if (depth == 1) {
                // Don't unlock i1, since we are going to try to
                // insert into that bucket after this ends. If i1==i2,
                // don't unlock anything
                if (i1 != i2) {
                    unlock(ti, i2);
                }
            } else {
                unlock_two(ti, i1, i2);
            }
            depth--;
        }

        return true;
    }

    cuckoo_status run_cuckoo(TableInfo* ti,
                             const size_t i1,
                             const size_t i2,
                             size_t &insert_bucket,
                             size_t &insert_slot) {
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
            cuckoo_path = new CuckooRecord[MAX_BFS_DEPTH+1];
        }

        //memset(cuckoo_path, 0, MAX_CUCKOO_COUNT * sizeof(CuckooRecord));

        /* We unlock i1 and i2 here, so that cuckoopath_move can swap
         * one of the slots in one of the buckets to its alternate
         * location. They have to be unlocked here, so that
         * cuckoopath_move can lock buckets in the correct order.
         * cuckoopath_move has to look at either i1 or i2 as its last
         * slot, and it will leave one of those buckets locked after
         * operating on it. This way, we know that if cuckoopath_move
         * succeeds, then the bucket it left locked is still empty. If
         * cuckoopath_move fails, we try again. This unlocking does
         * present a problem, where right after unlocking these
         * buckets, an expansion thread takes all the locks and
         * modifies table_info. If this occurs, then cuckoopath_move
         * would have operated on an old version of the table, so the
         * insert would be invalid. To check for this, we check that
         * ti == table_info.load() after cuckoopath_move. If the
         * comparison fails, that means that cuckoopath_move took the
         * locks after the expansion released them, so it is invalid
         * and we signal cuckoo_insert to try again. If the comparison
         * succeeds, then an expansion can't invalidate the insert,
         * since we still have one lock on the table */
        unlock_two(ti, i1, i2);

        bool done = false;
        while (!done) {
            int depth = cuckoopath_search(ti, cuckoo_path, i1, i2);

            if (depth < 0) {
                break;
            }

            bool success = cuckoopath_move(ti, cuckoo_path, depth);
            if (success) {
                // The bucket at insert_bucket (which equals i1 or i2)
                // should still be locked, but the other one shouldn't
                insert_bucket = cuckoo_path[0].bucket;
                insert_slot = cuckoo_path[0].slot;
                assert(!ti->locks_[lock_ind(insert_bucket)].try_lock());
                done = true;
                break;
            }
        }

#ifdef __linux__
#else
        delete [] cuckoo_path;
#endif
        if (!done) {
            return failure;
        } else if (ti != table_info.load()) {
            // Signals to cuckoo_insert to try again
            return failure_under_expansion;
        } else {
            return ok;
        }
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

                ti->buckets_[i].keys[j] = key;
                ti->buckets_[i].vals[j] = val;
                set_slot_used(ti, i, j);

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
                set_slot_empty(ti, i, j);
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
    cuckoo_status cuckoo_find(const key_type &key,
                              mapped_type &val) {
        uint32_t hv = hashed_key(key);
        TableInfo* ti;
        size_t i1, i2;
        snapshot_and_lock_two(hv, ti, i1, i2);
        if (try_read_from_bucket(ti, key, val, i1)) {
            unlock_two(ti, i1, i2);
            return ok;
        }

        bool res = try_read_from_bucket(ti, key, val, i2);
        unlock_two(ti, i1, i2);

        if (res) {
            return ok;
        }
        return failure_key_not_found;
    }

   /**
    * @brief insert (key, val) to bucket i1 or i2. The locks should be
    * acquired by the caller, but they are released here.
    *
    * @param key the key to insert
    * @param val the value associated with this key
    * @param val the value to return
    * @param i1  1st bucket index
    * @param i2  2nd bucket index
    *
    * @return ok on success
    */
    cuckoo_status cuckoo_insert(const key_type &key,
                                const mapped_type &val) {
        uint32_t hv = hashed_key(key);
        TableInfo* ti;
        size_t i1, i2;
        snapshot_and_lock_two(hv, ti, i1, i2);
        // try to add new key to bucket i1 first, then try bucket i2
        if (try_add_to_bucket(ti, key, val, i1)) {
            unlock_two(ti, i1, i2);
            return ok;
        }

        if (try_add_to_bucket(ti, key, val, i2)) {
            unlock_two(ti, i1, i2);
            return ok;
        }

        // we are unlucky, so let's perform cuckoo hashing
        size_t insert_bucket;
        size_t insert_slot;
        cuckoo_status st = run_cuckoo(ti, i1, i2, insert_bucket, insert_slot);
        // If run_cuckoo didn't return failure, insert_bucket is still
        // locked, but whichever bucket it isn't is unlocked, so we
        // only have to unlock insert_bucket. If i1 == i2, then
        // run_cuckoo didn't unlock anything, so we still unlock
        // insert_bucket.
        if (st == failure_under_expansion) {
            // Then the run_cuckoo operation operated on an old version
            // of the table, so we have to try again. We signal to the
            // calling insert method to try again by returning
            // failure_under_expansion
            unlock(ti, insert_bucket);
            return failure_under_expansion;
        } else if (st == ok) {
            assert(!ti->locks_[lock_ind(insert_bucket)].try_lock());
            assert(is_slot_empty(ti, insert_bucket, insert_slot));
            assert(insert_bucket == index_hash(ti, hv) || insert_bucket == alt_index(ti, hv, index_hash(ti, hv)));
            ti->buckets_[insert_bucket].keys[insert_slot] = key;
            ti->buckets_[insert_bucket].vals[insert_slot] = val;
            set_slot_used(ti, insert_bucket, insert_slot);
            ti->hashitems_.fetch_add(1);
            unlock(ti, insert_bucket);
            return ok;
        }

        assert(st == failure);
        DBG("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
            ti->hashpower_, ti->hashitems_.load(), 1.0 * ti->hashitems_.load() / SLOT_PER_BUCKET / hashsize(ti->hashpower_));
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
    cuckoo_status cuckoo_delete(const key_type &key) {
        uint32_t hv = hashed_key(key);
        TableInfo* ti;
        size_t i1, i2;
        snapshot_and_lock_two(hv, ti, i1, i2);
        if (try_del_from_bucket(ti, key, i1)) {
            unlock_two(ti, i1, i2);
            return ok;
        }

        bool res = try_del_from_bucket(ti, key, i2);
        unlock_two(ti, i1, i2);
        if (res) {
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
    /* This probably doesn't work yet
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
    */

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
       TableInfo* new_table_info = new TableInfo;
       try {
           new_table_info->hashpower_  = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
           new_table_info->tablesize_  = sizeof(Bucket) * hashsize(new_table_info->hashpower_);
           new_table_info->buckets_.reset(new Bucket[hashsize(new_table_info->hashpower_)]);
           new_table_info->locks_.reset(new std::mutex[kNumLocks]);
       } catch (std::bad_alloc&) {
           delete new_table_info;
           return failure;
       }

       table_info.store(new_table_info);
       cuckoo_clear(table_info.load());

       return ok;
   }


   cuckoo_status cuckoo_clear(TableInfo* ti) {
       for (size_t i = 0; i < hashsize(ti->hashpower_); i++) {
           ti->buckets_[i].flag = 0;
       }
       ti->hashitems_ = 0;
       expanding_.store(false);

       return ok;
   }


    // Expects the lock and the expanding_ variable to already be set.
    // We use cuckoo_expand_simple right now, so this function doesn't
    // work.
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
    * correct hash table by the end. Takes all the bucket locks before
    * doing anything. Since it requires all the locks, expansions will
    * not mess up other operations, though there is a small chance,
    * explained under snapshot_and_lock_two. In case multiple
    * expansions run concurrently, we check table_info after taking
    * all the locks. If it isn't equal to ti, we unlock everything and
    * return failure_under_expansion. This check may seem susceptible
    * to an ABA issue, where the address of the new table_info equals
    * the address of the old one, but it actualy isn't, since we
    * always allocate the new table_info before deleting the old
    * pointer, which we actually store to be deleted later. */
    static void insert_into_table(cuckoohash_map<Key, T, Hash>& new_map, const TableInfo* old_ti, size_t i, size_t end) {
        for (;i < end; i++) {
            for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
                if (!is_slot_empty(old_ti, i, j)) {
                    new_map.insert(old_ti->buckets_[i].keys[j], old_ti->buckets_[i].vals[j]);
                }
            }
        }
    }
    cuckoo_status cuckoo_expand_simple() {
        TableInfo* ti = snapshot_and_lock_all();
        if (ti == nullptr) {
            return failure_under_expansion;
        }

        // Creates a new hash table twice the size and adds all the
        // elements from the old buckets
        cuckoohash_map<Key, T, Hash> new_map(ti->hashpower_+1);
        const size_t threadnum = std::thread::hardware_concurrency();
        const size_t buckets_per_thread = hashsize(ti->hashpower_) / threadnum;
        std::vector<std::thread> insertion_threads(threadnum);
        for (size_t i = 0; i < threadnum-1; i++) {
            insertion_threads[i] = std::thread(insert_into_table, std::ref(new_map), ti, i*buckets_per_thread, (i+1)*buckets_per_thread);
        }
        insertion_threads[threadnum-1] = std::thread(insert_into_table, std::ref(new_map), ti, (threadnum-1)*buckets_per_thread, bucket_count());
        for (size_t i = 0; i < threadnum; i++) {
            insertion_threads[i].join();
        }

        // Sets this table_info to new_map's. It then sets new_map's
        // table_info to nullptr, so that it doesn't get deleted when
        // new_map goes out of scope
        table_info.store(new_map.table_info.load());
        new_map.table_info.store(nullptr);
        // Rather than deleting ti now, we store it in
        // old_table_infos. All the memory in old_table_infos will be
        // deleted when the hash table's destructor is called. This
        // probably wastes extra memory, but reference counting is too
        // expensive.
        old_table_infos.push_back(ti);
        unlock_all(ti);
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

   // Locks the entire table, retrying until snapshot_and_lock_all
   // succeeds. Then calculates end_pos and begin_pos, and sets index
   // and slot to the beginning or end of the table, based on the
   // boolean argument
   c_iterator(cuckoohash_map<K, V, H> *hm, bool is_end) {
       hm_ = hm;
       do {
           ti_ = hm_->snapshot_and_lock_all();
       } while (ti_ == nullptr);

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

   // Releases all the locks on the table, thereby freeing up the
   // table, but also invalidating all future operations with this
   // iterator
   void release() {
       if (has_table_lock) {
           hm_->unlock_all(ti_);
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
       assert(!hm_->is_slot_empty(ti_, index_, slot_));
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
