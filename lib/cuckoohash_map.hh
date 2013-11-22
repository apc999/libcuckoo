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
#include <list>
#include <cmath>
#include <limits>
#include <bitset>
#include <unistd.h>
#include <sched.h>
#include <stdint.h>

#include "cuckoohash_config.h"
#include "util.h"
#include "city.h"

class spinlock {
    std::atomic_flag lock_;
public:
    spinlock() {
        lock_.clear();
    }
    inline void lock() {
        while (lock_.test_and_set(std::memory_order_acquire));
    }
    inline void unlock() {
        lock_.clear(std::memory_order_release);
    }
} __attribute__((aligned(64)));

/* We forward-declare the iterator types, since they are needed in the
 * map. */
template <class K, class V, class H> class c_iterator;
template <class K, class V, class H> class mut_iterator;

/* cuckoohash_map is the primary class for the hash table. All the
 * functions that don't use iterators are implemented here. */
template <class Key, class T, class Hash = std::hash<Key> >
class cuckoohash_map {
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

    /* This is a hazard pointer, used to indicate which version of the
     * TableInfo is currently being used in the thread. Since
     * cuckoohash_map operations can run simultaneously in different
     * threads, this variable is thread local. Note that this variable
     * can be safely shared between different cuckoohash_map
     * instances, since multiple operations cannot occur
     * simultaneously in one thread. The hazard pointer variable
     * points to a pointer inside a global list of pointers, that each
     * map checks before deleting any old TableInfo pointers. */
    static __thread void** hazard_pointer;

    /* A GlobalHazardPointerList stores a list of pointers that cannot be
     * deleted by an expansion thread. Each thread gets its own node in
     * the list, whose data pointer it can modify without contention */
    class GlobalHazardPointerList {
        std::list<void*> hp_;
        std::mutex lock_;
    public:
        /* new_hazard_pointer creates and returns a new hazard pointer for
         * a thread. */
        void** new_hazard_pointer() {
            lock_.lock();
            hp_.emplace_back(nullptr);
            void** ret = &hp_.back();
            lock_.unlock();
            return ret;
        }

        /* delete_unused scans the list of hazard pointers, deleting any
         * pointers in old_pointers that aren't in this list. If it does
         * delete a pointer in old_pointers, it deletes that node from the
         * list. */
        template <class Ptr>
        void delete_unused(std::list<Ptr*>& old_pointers) {
            lock_.lock();
            auto it = old_pointers.begin();
            while (it != old_pointers.end()) {
                bool deleteable = true;
                for (auto hpit = hp_.cbegin(); hpit != hp_.cend(); hpit++) {
                    if (*hpit == *it) {
                        deleteable = false;
                        break;
                    }
                }
                if (deleteable) {
                    DBG("deleting %p\n", *it);
                    delete *it;
                    it = old_pointers.erase(it);
                } else {
                    it++;
                }
            }
            lock_.unlock();
        }
    };

    // As long as the thread_local hazard_pointer is static, which
    // means each template instantiation of a cuckoohash_map class
    // gets its own per-thread hazard pointer, then each template
    // instantiation of a cuckoohash_map class can get its own
    // global_hazard_pointers list, since different template
    // instantiations won't interfere with each other.
    static GlobalHazardPointerList global_hazard_pointers;

    /* check_hazard_pointer should be called before any public method
     * that loads a table snapshot. It checks that the thread local
     * hazard pointer pointer is not null, and gets a new pointer if
     * it is null. */
    static inline void check_hazard_pointer() {
        if (hazard_pointer == nullptr) {
            hazard_pointer = global_hazard_pointers.new_hazard_pointer();
        }
    }

    /* Once a function is finished with a version of the table, it
     * calls unset_hazard_pointer so that the pointer can be freed if
     * it needs to. */
    static inline void unset_hazard_pointer() {
        *hazard_pointer = nullptr;
    }

    /* counterid stores the per-thread counter index of each
     * thread. */
    static __thread int counterid;

    /* check_counterid checks if the counterid has already been
     * determined. If not, it assigns a counterid to the current
     * thread by picking a random core. This should be called at the
     * beginning of any function that changes the number of elements
     * in the table. */
    static inline void check_counterid() {
        if (counterid < 0) {
            counterid = cheap_rand() % kNumCores;
        }
    }

public:
    typedef Key               key_type;
    typedef std::pair<Key, T> value_type;
    typedef T                 mapped_type;
    typedef Hash              hasher;
    typedef size_t            size_type;

    explicit cuckoohash_map(size_t hashpower_init = HASHPOWER_DEFAULT) {
        if (cuckoo_init(hashpower_init) != ok) {
            throw std::runtime_error("Initialization failure");
        }
    }

    ~cuckoohash_map() {
        TableInfo *ti = table_info.load();
        if (ti != nullptr) {
            delete ti;
        }
        for (auto it = old_table_infos.cbegin(); it != old_table_infos.cend(); it++) {
            delete *it;
        }
    }

    /* clear removes all the elements in the hash table. */
    void clear() {
        cuckoo_clear();
    }

    /* size returns the number of items currently in the hash table.
     * Since it doesn't lock the table, elements can be inserted
     * during the computation, so the result may not necessarily be
     * exact. */
    size_type size() {
        check_hazard_pointer();
        const TableInfo *ti = snapshot_table_nolock();
        const size_t s = cuckoo_size(ti);
        unset_hazard_pointer();
        return s;
    }

    /* empty returns true if the table is empty. */
    bool empty() {
        return size() == 0;
    }

    /* hashpower returns the hashpower of the table, which is log2(the
     * number of buckets). */
    size_type hashpower() {
        check_hazard_pointer();
        TableInfo* ti = snapshot_table_nolock();
        const size_type hashpower = ti->hashpower_;
        unset_hazard_pointer();
        return hashpower;
    }

    /* bucket_count returns the number of buckets in the table. */
    size_type bucket_count() {
        check_hazard_pointer();
        TableInfo *ti = snapshot_table_nolock();
        size_type buckets = hashsize(ti->hashpower_);
        unset_hazard_pointer();
        return buckets;
    }

    /* load_factor returns the ratio of the number of items in the
     * bucket to the total number of slots in the table. */
    float load_factor() {
        check_hazard_pointer();
        const TableInfo *ti = snapshot_table_nolock();
        const float lf = cuckoo_loadfactor(ti);
        unset_hazard_pointer();
        return lf;
    }

    /* find searches through the table for the given key, and stores
     * the associated value it finds. Since it takes bucket locks, it
     * contends with writes. */
    bool find(const key_type& key, mapped_type& val) {
        check_hazard_pointer();
        uint64_t hv = hashed_key(key);
        TableInfo *ti;
        size_t i1, i2;
        snapshot_and_lock_two(hv, ti, i1, i2);

        const cuckoo_status st = cuckoo_find(key, val, hv, ti, i1, i2);
        unlock_two(ti, i1, i2);
        unset_hazard_pointer();

        return (st == ok);
    }

    /* This is the same as find, except it returns the value it finds,
     * throwing an exception if the key isn't in the table. */
    value_type find(const key_type& key) {
        mapped_type val;
        bool done = find(key, val);

        if (done) {
            return value_type(key, val);
        } else {
            throw std::out_of_range("key not found in table");
        }
    }

    /* insert puts the given key-value pair into the table. It first
     * checks that the key isn't already in the table, since the table
     * doesn't support duplicate keys. If the table is out of space,
     * insert will automatically expand until it can succeed. */
    bool insert(const key_type& key, const mapped_type& val) {
        check_hazard_pointer();
        check_counterid();
        uint64_t hv = hashed_key(key);
        TableInfo *ti;
        size_t i1, i2;
        snapshot_and_lock_two(hv, ti, i1, i2);
        cuckoo_status st = cuckoo_insert(key, val, hv, ti, i1, i2);
        while (st != ok) {
            // If the insert failed with failure_key_duplicated, it
            // returns here
            if (st == failure_key_duplicated) {
                unset_hazard_pointer();
                return false;
            }
            // If it failed with failure_under_expansion, the insert
            // operated on an old version of the table, so we just try
            // again. If it's failure_table_full, we have to expand
            // the table before trying again.
            if (st == failure_table_full) {
                if (cuckoo_expand_simple() == failure_under_expansion) {
                    DBG("expansion is on-going\n", NULL);
                }
            }
            snapshot_and_lock_two(hv, ti, i1, i2);
            st = cuckoo_insert(key, val, hv, ti, i1, i2);
        }
        unset_hazard_pointer();
        return true;
    }

    /* erase removes the given key from the table. If the key is not
     * there, it returns false. */
    bool erase(const key_type& key) {
        check_hazard_pointer();
        check_counterid();
        uint64_t hv = hashed_key(key);
        TableInfo *ti;
        size_t i1, i2;
        snapshot_and_lock_two(hv, ti, i1, i2);

        const cuckoo_status st = cuckoo_delete(key, ti, i1, i2);
        unlock_two(ti, i1, i2);
        unset_hazard_pointer();

        return (st == ok);
    }

    /* update changes the value of the given key. If the key is not
     * there, it returns false. */
    bool update(const key_type& key, const mapped_type& val) {
        check_hazard_pointer();
        uint64_t hv = hashed_key(key);
        TableInfo *ti;
        size_t i1, i2;
        snapshot_and_lock_two(hv, ti, i1, i2);

        const cuckoo_status st = cuckoo_update(key, val, ti, i1, i2);
        unlock_two(ti, i1, i2);
        unset_hazard_pointer();

        return (st == ok);
    }

    /* expand will double the size of the existing table. */
    bool expand() {
        check_hazard_pointer();
        const cuckoo_status st = cuckoo_expand_simple();
        unset_hazard_pointer();
        return (st == ok);
    }

    /* report prints out some information about the current state of
     * the table*/
    void report() {
        check_hazard_pointer();
        TableInfo *ti = snapshot_table_nolock();
        const size_t s = cuckoo_size(ti);
        printf("total number of items %zu\n", s);
        printf("total size %zu Bytes, or %.2f MB\n",
               ti->tablesize_, (float) ti->tablesize_ / (1 << 20));
        printf("load factor %.4f\n", 1.0 * s / SLOT_PER_BUCKET / hashsize(ti->hashpower_));
        unset_hazard_pointer();
    }

private:
    /* The Bucket type holds SLOT_PER_BUCKET keys and values, and a
     * occupied bitset, which indicates whether the slot at the given
     * bit index is in the table or not. */
    typedef struct {
        std::bitset<SLOT_PER_BUCKET> occupied;
        key_type keys[SLOT_PER_BUCKET];
        mapped_type vals[SLOT_PER_BUCKET];
    } Bucket;

    /* cacheint is a cache-aligned atomic integer type. */
    struct cacheint {
        std::atomic<size_t> num;
        cacheint() {}
        cacheint(cacheint&& x) {
            num.store(x.num.load());
        }
    } __attribute__((aligned(64)));

    // An alias for the type of lock we are using
    typedef spinlock locktype;

    /* TableInfo contains the entire state of the hashtable. We
     * allocate one TableInfo pointer per hash table and store all of
     * the table memory in it, so that all the data can be atomically
     * swapped during expansion. */
    struct TableInfo {
        // size of the table in bytes
        size_t tablesize_;

        // 2**hashpower is the number of buckets
        size_t hashpower_;

        // unique pointer to the array of buckets
        std::unique_ptr<Bucket[]> buckets_;

        // unique pointer to the array of mutexes
        std::unique_ptr<locktype[]> locks_;

        // per-core counters for the number of inserts and deletes
        std::vector<cacheint> num_inserts;
        std::vector<cacheint> num_deletes;
    };
    std::atomic<TableInfo*> table_info;

    /* old_table_infos holds pointers to old TableInfos that were
     * replaced during expansion. This keeps the memory alive for any
     * leftover operations, until they are deleted by the global
     * hazard pointer manager. */
    std::list<TableInfo*> old_table_infos;

    /* lock locks the given bucket index. */
    static inline void lock(const TableInfo *ti, const size_t i) {
        ti->locks_[lock_ind(i)].lock();
    }

    /* unlock unlocks the given bucket index. */
    static inline void unlock(const TableInfo *ti, const size_t i) {
        ti->locks_[lock_ind(i)].unlock();
    }

    /* lock_two locks the two bucket indexes, always locking the
     * earlier index first to avoid deadlock. If the two indexes are
     * the same, it just locks one. */
    static inline void lock_two(const TableInfo *ti, size_t i1, size_t i2) {
        i1 = lock_ind(i1);
        i2 = lock_ind(i2);
        if (i1 < i2) {
            ti->locks_[i1].lock();
            ti->locks_[i2].lock();
        } else if (i2 < i1) {
            ti->locks_[i2].lock();
            ti->locks_[i1].lock();
        } else {
            ti->locks_[i1].lock();
        }
    }

    /* unlock_two unlocks both of the given bucket indexes, or only
     * one if they are equal. Order doesn't matter here. */
    static inline void unlock_two(const TableInfo *ti, size_t i1, size_t i2) {
        i1 = lock_ind(i1);
        i2 = lock_ind(i2);
        ti->locks_[i1].unlock();
        if (i1 != i2) {
            ti->locks_[i2].unlock();
        }
    }

    /* lock_three locks the three bucket indexes in numerical
     * order. */
    static inline void lock_three(const TableInfo *ti, size_t i1,
                                  size_t i2, size_t i3) {
        i1 = lock_ind(i1);
        i2 = lock_ind(i2);
        i3 = lock_ind(i3);
        // If any are the same, we just run lock_two
        if (i1 == i2) {
            lock_two(ti, i1, i3);
        } else if (i2 == i3) {
            lock_two(ti, i1, i3);
        } else if (i1 == i3) {
            lock_two(ti, i1, i2);
        } else {
            if (i1 < i2) {
                if (i2 < i3) {
                    ti->locks_[i1].lock();
                    ti->locks_[i2].lock();
                    ti->locks_[i3].lock();
                } else if (i1 < i3) {
                    ti->locks_[i1].lock();
                    ti->locks_[i3].lock();
                    ti->locks_[i2].lock();
                } else {
                    ti->locks_[i3].lock();
                    ti->locks_[i1].lock();
                    ti->locks_[i2].lock();
                }
            } else if (i2 < i3) {
                if (i1 < i3) {
                    ti->locks_[i2].lock();
                    ti->locks_[i1].lock();
                    ti->locks_[i3].lock();
                } else {
                    ti->locks_[i2].lock();
                    ti->locks_[i3].lock();
                    ti->locks_[i1].lock();
                }
            } else {
                ti->locks_[i3].lock();
                ti->locks_[i2].lock();
                ti->locks_[i1].lock();
            }
        }
    }

    /* unlock_three unlocks the three given buckets */
    static inline void unlock_three(const TableInfo *ti, size_t i1,
                                    size_t i2, size_t i3) {
        i1 = lock_ind(i1);
        i2 = lock_ind(i2);
        i3 = lock_ind(i3);
        ti->locks_[i1].unlock();
        if (i2 != i1) {
            ti->locks_[i2].unlock();
        }
        if (i3 != i1 && i3 != i2) {
            ti->locks_[i3].unlock();
        }
    }


    /* snapshot_table_nolock loads the table info pointer and sets the
     * hazard pointer, whithout locking anything. There is a
     * possibility that after loading a snapshot and setting the
     * hazard pointer, an expansion runs and create a new version of
     * the table, leaving the old one for deletion. To deal with that,
     * we check that the table_info we loaded is the same as the
     * current one, and if it isn't, we try again. Whenever we check
     * if (ti != table_info.load()) after setting the hazard pointer,
     * there is an ABA issue, where the address of the new table_info
     * equals the address of a previously deleted one, however it
     * doesn't matter, since we would still be looking at the most
     * recent table_info in that case. */
    inline TableInfo* snapshot_table_nolock() {
        TableInfo *ti;
    TryAcquire:
        ti = table_info.load();
        *hazard_pointer = static_cast<void*>(ti);
        if (ti != table_info.load()) {
            goto TryAcquire;
        }
        return ti;
    }

    /* snapshot_and_lock_two loads the table_info pointer and locks
     * the buckets associated with the given hash value. It stores the
     * table_info and the two locked buckets in reference variables.
     * Since the positions of the bucket locks depends on the number
     * of buckets in the table, the table_info pointer needs to be
     * grabbed first. */
    inline void snapshot_and_lock_two(const uint64_t hv, TableInfo*& ti,
                                      size_t& i1, size_t& i2) {
    TryAcquire:
        ti = table_info.load();
        *hazard_pointer = static_cast<void*>(ti);
        if (ti != table_info.load()) {
            goto TryAcquire;
        }
        i1 = index_hash(ti, hv);
        i2 = alt_index(ti, hv, i1);
        lock_two(ti, i1, i2);
        if (ti != table_info.load()) {
            unlock_two(ti, i1, i2);
            goto TryAcquire;
        }
    }

    /* snapshot_and_lock_all is similar to snapshot_and_lock_two,
     * except that it takes all the locks and returns nullptr if the
     * loaded table_info doesn't match the most current one, rather
     * than retrying. */
    inline TableInfo *snapshot_and_lock_all() {
        TableInfo *ti = table_info.load();
        *hazard_pointer = static_cast<void*>(ti);
        if (ti != table_info.load()) {
            unset_hazard_pointer();
            return nullptr;
        }
        for (size_t i = 0; i < kNumLocks; i++) {
            ti->locks_[i].lock();
        }
        if (ti != table_info.load()) {
            unlock_all(ti);
            unset_hazard_pointer();
            return nullptr;
        }
        return ti;
    }

    /* unlock_all releases all the locks */
    inline void unlock_all(TableInfo *ti) {
        for (size_t i = 0; i < kNumLocks; i++) {
            ti->locks_[i].unlock();
        }
    }

    // key size in bytes
    static const size_t kKeySize = sizeof(key_type);

    // value size in bytes
    static const size_t kValueSize = sizeof(mapped_type);

    // size of a bucket in bytes
    static const size_t kBucketSize = sizeof(Bucket);

    // number of locks in the locks_ array
    static const size_t kNumLocks = 1 << 13;

    // number of cores on the machine
    static const size_t kNumCores;

    /* lock_ind converts an index into buckets_ to an index into
     * locks_. */
    static inline size_t lock_ind(const size_t bucket_ind) {
        return bucket_ind & (kNumLocks - 1);
    }

    /* hashsize returns the number of buckets corresponding to a given
     * hashpower. */
    static inline size_t hashsize(const size_t hashpower) {
        return 1U << hashpower;
    }

    /* hashmask returns the bitmask for the buckets array
     * corresponding to a given hashpower. */
    static inline size_t hashmask(const size_t hashpower) {
        return hashsize(hashpower) - 1;
    }

    /* hashed_key hashes the given key. */
    static inline uint64_t hashed_key(const key_type &key) {
        // return hasher()(key);
        return CityHash64((const char*) &key, kKeySize);
    }

    /* index_hash returns the first possible bucket that the given
     * hashed key could be. */
    static inline size_t index_hash(const TableInfo *ti, const uint64_t hv) {
        return hv & hashmask(ti->hashpower_);
    }

    /* alt_index returns the other possible bucket that the given
     * hashed key could be. It takes the first possible bucket as a
     * parameter. Note that this function will return the first
     * possible bucket if index is the second possible bucket, so
     * alt_index(ti, hv, alt_index(ti, hv, index_hash(ti, hv))) ==
     * index_hash(ti, hv). */
    static inline size_t alt_index(const TableInfo *ti,
                                   const uint64_t hv,
                                   const size_t index) {
        // ensure tag is nonzero for the multiply
        const uint64_t tag = (hv >> 24) + 1;
        /* 0x5bd1e995 is the hash constant from MurmurHash2 */
        return (index ^ (tag * 0x5bd1e995)) & hashmask(ti->hashpower_);
    }

    /* CuckooRecord holds one position in a cuckoo path. */
    typedef struct  {
        size_t   bucket;
        size_t   slot;
        key_type key;
    }  CuckooRecord;

    /* b_slot holds the information for a BFS path through the
     * table */
    struct b_slot {
        // The bucket of the last item in the path
        size_t bucket;
        // a compressed representation of the slots for each of the
        // buckets in the path.
        size_t pathcode;
        // static_assert(pow(SLOT_PER_BUCKET, MAX_BFS_DEPTH+1) <
        //               std::numeric_limits<decltype(pathcode)>::max(),
        //               "pathcode may not be large enough to encode a cuckoo path");
        // The 0-indexed position in the cuckoo path this slot
        // occupies
        int depth;
        b_slot() {}
        b_slot(const size_t b, const size_t p, const int d)
            : bucket(b), pathcode(p), depth(d) {}
    } __attribute__((__packed__));

    /* b_queue is the queue used to store b_slots for BFS cuckoo
     * hashing. */
    class b_queue {
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

    /* slot_search searches for a cuckoo path using breadth-first
       search. It starts with the i1 and i2 buckets, and, until it finds
       a bucket with an empty slot, adds each slot of the bucket in the
       b_slot. If the queue runs out of space, it fails. */
    static b_slot slot_search(const TableInfo *ti, const size_t i1,
                              const size_t i2) {
        b_queue q;
        // The initial pathcode informs cuckoopath_search which bucket
        // the path starts on
        q.enqueue(b_slot(i1, 0, 0));
        q.enqueue(b_slot(i2, 1, 0));
        while (q.not_full()) {
            b_slot x = q.dequeue();
            // Picks a random slot to start from
            const size_t start = (cheap_rand() >> 20) % SLOT_PER_BUCKET;
            for (size_t i = 0; i < SLOT_PER_BUCKET && q.not_full(); i++) {
                size_t slot = (i+start) % SLOT_PER_BUCKET;
                // Create a new b_slot item, that represents the
                // bucket we would look at after searching x.bucket
                // for empty slots. This means we completely skip
                // searching i1 and i2, but they should have already
                // been searched by cuckoo_insert, so that's okay.
                const uint64_t hv = hashed_key(ti->buckets_[x.bucket].keys[slot]);
                b_slot y(alt_index(ti, hv, x.bucket),
                         x.pathcode * SLOT_PER_BUCKET + slot, x.depth+1);

                // Check if any of the slots in the prospective bucket
                // are empty, and, if so, return that b_slot. We lock
                // the bucket so that no changes occur while
                // iterating.
                lock(ti, y.bucket);
                for (int m = 0; m < SLOT_PER_BUCKET; m++) {
                    const size_t j = (start+m) % SLOT_PER_BUCKET;
                    if (!ti->buckets_[y.bucket].occupied.test(j)) {
                        y.pathcode = y.pathcode * SLOT_PER_BUCKET + j;
                        unlock(ti, y.bucket);
                        return y;
                    }
                }
                unlock(ti, y.bucket);

                // No empty slots were found, so we push this onto the
                // queue
                if (y.depth != MAX_BFS_DEPTH) {
                    q.enqueue(y);
                }
            }
        }
        // We didn't find a short-enough cuckoo path, so the queue ran
        // out of space. Return a failure value.
        return b_slot(0, 0, -1);
    }

    /* cuckoopath_search finds a cuckoo path from one of the starting
     * buckets to an empty slot in another bucket. It returns the
     * depth of the discovered cuckoo path on success, and -1 on
     * failure. Since it doesn't take locks on the buckets it
     * searches, the data can change between this function and
     * cuckoopath_move. Thus cuckoopath_move checks that the data
     * matches the cuckoo path before changing it. */
    static int cuckoopath_search(const TableInfo *ti, CuckooRecord* cuckoo_path,
                                 const size_t i1, const size_t i2) {
        b_slot x = slot_search(ti, i1, i2);
        if (x.depth == -1) {
            DBG("max cuckoo achieved, abort\n", NULL);
            return -1;
        }
        // Fill in the cuckoo path slots from the end to the beginning
        for (int i = x.depth; i >= 0; i--) {
            cuckoo_path[i].slot = x.pathcode % SLOT_PER_BUCKET;
            x.pathcode /= SLOT_PER_BUCKET;
        }
        /* Fill in the cuckoo_path buckets and keys from the beginning
         * to the end, using the final pathcode to figure out which
         * bucket the path starts on. Since data could have been
         * modified between slot_search and the computation of the
         * cuckoo path, this could be an invalid cuckoo_path. */
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
            const uint64_t prevhv = hashed_key(prev->key);
            assert(prev->bucket == index_hash(ti, prevhv) ||
                   prev->bucket == alt_index(ti, prevhv, index_hash(ti, prevhv)));
            // We get the bucket that this slot is on by computing the
            // alternate index of the previous bucket
            curr->bucket = alt_index(ti, prevhv, prev->bucket);
            curr->key = ti->buckets_[curr->bucket].keys[curr->slot];
        }
        return x.depth;
    }


    /* cuckoopath_move moves keys along the given cuckoo path in order
     * to make an empty slot in one of the buckets in cuckoo_insert.
     * Before the start of this function, the two insert-locked
     * buckets were unlocked in run_cuckoo. At the end of the
     * function, if the function returns true (success), then the last
     * bucket it looks at (which is either i1 or i2 in run_cuckoo)
     * remains locked. If the function is unsuccessful, then both
     * insert-locked buckets will be unlocked. */
    static bool cuckoopath_move(TableInfo *ti, CuckooRecord* cuckoo_path,
                                size_t depth, const size_t i1, const size_t i2) {
        if (depth == 0) {
            /* There is a chance that depth == 0, when
             * try_add_to_bucket sees i1 and i2 as full and
             * cuckoopath_search finds one empty. In this case, we
             * lock both buckets. If the bucket that cuckoopath_search
             * found empty isn't empty anymore, we unlock them and
             * return false. Otherwise, the bucket is empty and
             * insertable, so we hold the locks and return true. */
            const size_t bucket = cuckoo_path[0].bucket;
            assert(bucket == i1 || bucket == i2);
            lock_two(ti, i1, i2);
            if (!ti->buckets_[bucket].occupied[cuckoo_path[0].slot]) {
                return true;
            } else {
                unlock_two(ti, i1, i2);
                return false;
            }
        }

        while (depth > 0) {
            CuckooRecord *from = cuckoo_path + depth - 1;
            CuckooRecord *to   = cuckoo_path + depth;
            size_t fb = from->bucket;
            size_t fs = from->slot;
            size_t tb = to->bucket;
            size_t ts = to->slot;

            size_t ob = 0;
            if (depth == 1) {
                /* Even though we are only swapping out of i1 or i2,
                 * we have to lock both of them along with the slot we
                 * are swapping to, since at the end of this function,
                 * i1 and i2 must be locked. */
                ob = (fb == i1) ? i2 : i1;
                lock_three(ti, fb, tb, ob);
            } else {
                lock_two(ti, fb, tb);
            }

            /* We plan to kick out fs, but let's check if it is still
             * there; there's a small chance we've gotten scooped by a
             * later cuckoo. If that happened, just... try again. Also
             * the slot we are filling in may have already been filled
             * in by another thread, or the slot we are moving from
             * may be empty, both of which invalidate the swap. */
            if (ti->buckets_[fb].keys[fs] != from->key ||
                ti->buckets_[tb].occupied[ts] ||
                !ti->buckets_[fb].occupied[fs]) {
                if (depth == 1) {
                    unlock_three(ti, fb, tb, ob);
                } else {
                    unlock_two(ti, fb, tb);
                }
                return false;
            }

            ti->buckets_[tb].keys[ts] = ti->buckets_[fb].keys[fs];
            ti->buckets_[tb].vals[ts] = ti->buckets_[fb].vals[fs];
            ti->buckets_[tb].occupied.set(ts);
            ti->buckets_[fb].occupied.reset(fs);
            if (depth == 1) {
                // Don't unlock fb or ob, since they are needed in
                // cuckoo_insert. Only unlock tb if it isn't equal to
                // fb or ob.
                if (tb != fb && tb != ob) {
                    unlock(ti, tb);
                }
            } else {
                unlock_two(ti, fb, tb);
            }
            depth--;
        }
        return true;
    }

    /* run_cuckoo performs cuckoo hashing on the table in an attempt
     * to free up a slot on either i1 or i2. On success, the bucket
     * and slot that was freed up is stored in insert_bucket and
     * insert_slot. In order to perform the search and the swaps, it
     * has to unlock both i1 and i2, which can lead to certain
     * concurrency issues, the details of which are explained in the
     * function. If run_cuckoo returns ok (success), then the slot it
     * freed up is still locked. Otherwise it is unlocked. */
    cuckoo_status run_cuckoo(TableInfo *ti, const size_t i1, const size_t i2,
                             size_t &insert_bucket, size_t &insert_slot) {

        CuckooRecord cuckoo_path[MAX_BFS_DEPTH+1];

        // We must unlock i1 and i2 here, so that cuckoopath_search
        // and cuckoopath_move can lock buckets as desired without
        // deadlock. cuckoopath_move has to look at either i1 or i2 as
        // its last slot, and it will lock both buckets and leave them
        // locked after finishing. This way, we know that if
        // cuckoopath_move succeeds, then the buckets needed for
        // insertion are still locked. If cuckoopath_move fails, the
        // buckets are unlocked and we try again. This unlocking does
        // present two problems. The first is that another insert on
        // the same key runs and, finding that the key isn't in the
        // table, inserts the key into the table. Then we insert the
        // key into the table, causing a duplication. To check for
        // this, we search i1 and i2 for the key we are trying to
        // insert before doing so (this is done in cuckoo_insert, and
        // requires that both i1 and i2 are locked). Another problem
        // is that an expansion runs and changes table_info, meaning
        // the cuckoopath_move and cuckoo_insert would have operated
        // on an old version of the table, so the insert would be
        // invalid. For this, we check that ti == table_info.load()
        // after cuckoopath_move, signaling to the outer insert to try
        // again if the comparison fails.
        unlock_two(ti, i1, i2);

        bool done = false;
        while (!done) {
            int depth = cuckoopath_search(ti, cuckoo_path, i1, i2);
            if (depth < 0) {
                break;
            }

            if (cuckoopath_move(ti, cuckoo_path, depth, i1, i2)) {
                insert_bucket = cuckoo_path[0].bucket;
                insert_slot = cuckoo_path[0].slot;
                assert(insert_bucket == i1 || insert_bucket == i2);
                assert(!ti->locks_[lock_ind(i1)].try_lock());
                assert(!ti->locks_[lock_ind(i2)].try_lock());
                assert(!ti->buckets_[insert_bucket].occupied[insert_slot]);
                done = true;
                break;
            }
        }

        if (!done) {
            return failure;
        } else if (ti != table_info.load()) {
            // Unlock i1 and i2 and signal to cuckoo_insert to try
            // again. Since we set the hazard pointer to be ti, this
            // check isn't susceptible to an ABA issue, since a new
            // pointer can't have the same address as ti.
            unlock_two(ti, i1, i2);
            return failure_under_expansion;
        }
        return ok;
    }

    /* try_read_from-bucket will search the bucket for the given key
     * and store the associated value if it finds it. */
    static bool try_read_from_bucket(const TableInfo *ti, const key_type &key,
                                     mapped_type &val, const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            if (!ti->buckets_[i].occupied[j]) {
                continue;
            }
            if (key == ti->buckets_[i].keys[j]) {
                val = ti->buckets_[i].vals[j];
                return true;
            }
        }
        return false;
    }

    /* add_to_bucket will insert the given key-value pair into the
     * slot. */
    static inline void add_to_bucket(TableInfo *ti, const key_type &key,
                                     const mapped_type &val, const size_t i,
                                     const size_t j) {
        assert(!ti->buckets_[i].occupied[j]);
        ti->buckets_[i].keys[j] = key;
        ti->buckets_[i].vals[j] = val;
        ti->buckets_[i].occupied.set(j);
        ti->num_inserts[counterid].num.fetch_add(1, std::memory_order_relaxed);
    }

    /* try_add_to_bucket will search the bucket and store the index of
     * an empty slot if it finds one, or -1 if it doesn't. Regardless,
     * it will search the entire bucket and return false if it finds
     * the key already in the table (duplicate key error) and true
     * otherwise. */
    static bool try_add_to_bucket(TableInfo *ti,
                                  const key_type    &key,
                                  const mapped_type &val,
                                  const size_t i,
                                  int& j) {
        j = -1;
        bool found = false;
        for (size_t k = 0; k < SLOT_PER_BUCKET; k++) {
            if (ti->buckets_[i].occupied[k]) {
                if (key == ti->buckets_[i].keys[k]) {
                    return false;
                }
            } else if (!found) {
                found = true;
                j = k;
            }
        }
        return true;
    }


    /* try_del_from_bucket will search the bucket for the given key,
     * and set the slot of the key to empty if it finds it. */
    static bool try_del_from_bucket(TableInfo *ti,
                                    const key_type &key,
                                    const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            if (!ti->buckets_[i].occupied[j]) {
                continue;
            }
            if (ti->buckets_[i].keys[j] == key) {
                ti->buckets_[i].occupied.reset(j);
                ti->num_deletes[counterid].num.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }
        return false;
    }

    /* try_update_bucket will search the bucket for the given key and
     * change its associated value if it finds it. */
    static bool try_update_bucket(TableInfo *ti,
                                  const key_type &key,
                                  const mapped_type &value,
                                  const size_t i) {
        for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
            if (ti->buckets_[i].occupied[j] && ti->buckets_[i].keys[j] == key) {
                ti->buckets_[i].vals[j] = value;
                return true;
            }
        }
        return false;
    }

    /* cuckoo_find searches the table for the given key and value,
     * storing the value in the val if it finds the key. It expects
     * the locks to be taken and released outside the function. */
    static cuckoo_status cuckoo_find(const key_type &key,
                                     mapped_type &val,
                                     const uint64_t hv,
                                     const TableInfo *ti,
                                     const size_t i1,
                                     const size_t i2) {
        if (try_read_from_bucket(ti, key, val, i1)) {
            return ok;
        }
        if (try_read_from_bucket(ti, key, val, i2)) {
            return ok;
        }
        return failure_key_not_found;
    }

    /* cuckoo_insert tries to insert the given key-value pair into an
     * empty slot in i1 or i2, performing cuckoo hashing if necessary.
     * It expects the locks to be taken outside the function, but they
     * are released here, since different scenarios require different
     * handling of the locks. Before inserting, it checks that the key
     * isn't already in the table. cuckoo hashing presents multiple
     * concurrency issues, which are explained in the function. */
    cuckoo_status cuckoo_insert(const key_type &key,
                                const mapped_type &val,
                                const uint64_t hv,
                                TableInfo *ti,
                                const size_t i1,
                                const size_t i2) {
        mapped_type oldval;
        int res1, res2;
        if (!try_add_to_bucket(ti, key, val, i1, res1)) {
            unlock_two(ti, i1, i2);
            return failure_key_duplicated;
        }
        if (res1 != -1) {
            if (try_read_from_bucket(ti, key, oldval, i2)) {
                unlock_two(ti, i1, i2);
                return failure_key_duplicated;
            } else {
                add_to_bucket(ti, key, val, i1, res1);
                unlock_two(ti, i1, i2);
                return ok;
            }
        }
        if (!try_add_to_bucket(ti, key, val, i2, res2)) {
            unlock_two(ti, i1, i2);
            return failure_key_duplicated;
        }
        if (res2 != -1) {
            add_to_bucket(ti, key, val, i2, res2);
            unlock_two(ti, i1, i2);
            return ok;
        }

        // we are unlucky, so let's perform cuckoo hashing
        size_t insert_bucket;
        size_t insert_slot;
        cuckoo_status st = run_cuckoo(ti, i1, i2, insert_bucket, insert_slot);
        if (st == failure_under_expansion) {
            /* The run_cuckoo operation operated on an old version of
             * the table, so we have to try again. We signal to the
             * calling insert method to try again by returning
             * failure_under_expansion. */
            return failure_under_expansion;
        } else if (st == ok) {
            assert(!ti->locks_[lock_ind(i1)].try_lock());
            assert(!ti->locks_[lock_ind(i2)].try_lock());
            assert(!ti->buckets_[insert_bucket].occupied[insert_slot]);
            assert(insert_bucket == index_hash(ti, hv) || insert_bucket == alt_index(ti, hv, index_hash(ti, hv)));
            /* Since we unlocked the buckets during run_cuckoo,
             * another insert could have inserted the same key into
             * either i1 or i2, so we check for that before doing the
             * insert. */
            if (cuckoo_find(key, oldval, hv, ti, i1, i2) == ok) {
                return failure_key_duplicated;
            }
            add_to_bucket(ti, key, val, insert_bucket, insert_slot);
            unlock_two(ti, i1, i2);
            return ok;
        }

        assert(st == failure);
        DBG("hash table is full (hashpower = %zu, hash_items = %zu, load factor = %.2f), need to increase hashpower\n",
            ti->hashpower_, cuckoo_size(ti), cuckoo_loadfactor(ti));
        return failure_table_full;
    }

    /* cuckoo_delete searches the table for the given key and sets the
     * slot with that key to empty if it finds it. It expects the
     * locks to be taken and released outside the function. */
    cuckoo_status cuckoo_delete(const key_type &key,
                                TableInfo *ti,
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

    /* cuckoo_update searches the table for the given key and updates
     * its value if it finds it. It expects the locks to be taken and
     * released outside the function. */
    cuckoo_status cuckoo_update(const key_type &key,
                                const mapped_type &val,
                                TableInfo *ti,
                                const size_t i1,
                                const size_t i2) {

        if (try_update_bucket(ti, key, val, i1)) {
            return ok;
        }
        if (try_update_bucket(ti, key, val, i2)) {
            return ok;
        }
        return failure_key_not_found;
    }

    /* cuckoo_init initializes the hashtable, given an initial
     * hashpower as the argument. It allocates one cacheint for each
     * core in num_inserts and num_deletes. */
    cuckoo_status cuckoo_init(const size_t hashtable_init) {
        TableInfo *new_table_info = new TableInfo;
        try {
            new_table_info->hashpower_ = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
            new_table_info->tablesize_ = sizeof(Bucket) * hashsize(new_table_info->hashpower_);
            new_table_info->buckets_.reset(new Bucket[hashsize(new_table_info->hashpower_)]);
            new_table_info->locks_.reset(new locktype[kNumLocks]);
            new_table_info->num_inserts.resize(kNumCores);
            new_table_info->num_deletes.resize(kNumCores);
        } catch (std::bad_alloc&) {
            delete new_table_info;
            return failure;
        }

        table_info.store(new_table_info);
        cuckoo_clear();

        return ok;
    }

    /* cuckoo_clear empties the table. */
    cuckoo_status cuckoo_clear() {
        check_hazard_pointer();
        TableInfo *ti = snapshot_and_lock_all();
        if (ti == nullptr) {
            return cuckoo_clear();
        }

        for (size_t i = 0; i < hashsize(ti->hashpower_); i++) {
            ti->buckets_[i].occupied.reset();
        }

        for (size_t i = 0; i < ti->num_inserts.size(); i++) {
            ti->num_inserts[i].num.store(0);
            ti->num_deletes[i].num.store(0);
        }

        unlock_all(ti);
        unset_hazard_pointer();
        return ok;
    }

    /* cuckoo_size returns the number of elements in the given
     * table. */
    size_t cuckoo_size(const TableInfo *ti) {
        size_t inserts = 0;
        size_t deletes = 0;
        for (size_t i = 0; i < ti->num_inserts.size(); i++) {
            inserts += ti->num_inserts[i].num.load();
            deletes += ti->num_deletes[i].num.load();
        }
        return inserts-deletes;
    }

    /* cuckoo_loadfactor returns the load factor of the given table. */
    float cuckoo_loadfactor(const TableInfo *ti) {
        return 1.0 * cuckoo_size(ti) / SLOT_PER_BUCKET / hashsize(ti->hashpower_);
    }

    /* insert_into_table is a helper function used by
     * cuckoo_expand_simple to fill up the new table. */
    static void insert_into_table(cuckoohash_map<Key, T, Hash>& new_map, const TableInfo *old_ti, size_t i, size_t end) {
        for (;i < end; i++) {
            for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
                if (old_ti->buckets_[i].occupied[j]) {
                    new_map.insert(old_ti->buckets_[i].keys[j], old_ti->buckets_[i].vals[j]);
                }
            }
        }
    }

    /* cuckoo_expand_simple is a simpler version of expansion than
     * cuckoo_expand, which will double the size of the existing hash
     * table. It needs to take all the bucket locks, since no other
     * operations can change the table during expansion. */
    cuckoo_status cuckoo_expand_simple() {
        TableInfo *ti = snapshot_and_lock_all();
        if (ti == nullptr) {
            return failure_under_expansion;
        }

        // Creates a new hash table twice the size and adds all the
        // elements from the old buckets
        cuckoohash_map<Key, T, Hash> new_map(ti->hashpower_+1);
        const size_t threadnum = kNumCores;
        const size_t buckets_per_thread = hashsize(ti->hashpower_) / threadnum;
        std::vector<std::thread> insertion_threads(threadnum);
        for (size_t i = 0; i < threadnum-1; i++) {
            insertion_threads[i] = std::thread(
                insert_into_table, std::ref(new_map),
                ti, i*buckets_per_thread, (i+1)*buckets_per_thread);
        }
        insertion_threads[threadnum-1] = std::thread(
            insert_into_table, std::ref(new_map),
            ti, (threadnum-1)*buckets_per_thread, hashsize(ti->hashpower_));
        for (size_t i = 0; i < threadnum; i++) {
            insertion_threads[i].join();
        }

        // Sets this table_info to new_map's. It then sets new_map's
        // table_info to nullptr, so that it doesn't get deleted when
        // new_map goes out of scope
        table_info.store(new_map.table_info.load());
        new_map.table_info.store(nullptr);
        // Rather than deleting ti now, we store it in
        // old_table_infos. The hazard pointer manager will delete it
        // if no other threads are using the pointer.
        old_table_infos.push_back(ti);
        unlock_all(ti);
        unset_hazard_pointer();
        global_hazard_pointers.delete_unused(old_table_infos);
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

// Initializing the static members
template <class Key, class T, class Hash>
__thread void** cuckoohash_map<Key, T, Hash>::hazard_pointer = nullptr;

template <class Key, class T, class Hash>
__thread int cuckoohash_map<Key, T, Hash>::counterid = -1;

template <class Key, class T, class Hash>
typename cuckoohash_map<Key, T, Hash>::GlobalHazardPointerList
cuckoohash_map<Key, T, Hash>::global_hazard_pointers;

template <class Key, class T, class Hash>
const size_t cuckoohash_map<Key, T, Hash>::kNumCores =
    std::thread::hardware_concurrency() == 0 ?
    sysconf(_SC_NPROCESSORS_ONLN) : std::thread::hardware_concurrency();

/* An iterator through the table that is thread safe. For the duration
 * of its existence, it takes all the locks on the table it is given,
 * thereby ensuring that no other threads can modify the table while
 * the iterator is in use. It allows movement forward and backward
 * through the table as well as dereferencing items in the table. It
 * maintains the assertion that an iterator is either an end iterator
 * (which points past the end of the table), or it points to a filled
 * slot. As soon as the iterator looses its lock on the table, all
 * operations will throw an exception. Even though this class is
 * available publicly, it is meant to be only created using the
 * iterator functions present in cuckohash_map. */
template <class K, class V, class H>
class c_iterator {
public:
    /* The constructor locks the entire table, retrying until
     * snapshot_and_lock_all succeeds. Then it calculates end_pos and
     * begin_pos and sets index and slot to the beginning or end of
     * the table, based on the boolean argument. */
    c_iterator(cuckoohash_map<K, V, H> *hm, bool is_end) {
        cuckoohash_map<K, V, H>::check_hazard_pointer();
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

    /* This is an rvalue-reference constructor that takes the lock from
     * the argument and copies its state. */
    c_iterator(c_iterator&& it) {
        if (this == &it) {
            return;
        }
        memcpy(this, &it, sizeof(c_iterator));
        it.has_table_lock = false;
    }

    /* This does the same thing as the rvalue-reference
     * constructor. */
    c_iterator* operator=(c_iterator&& it) {
        if (this == &it) {
            return this;
        }
        memcpy(this, &it, sizeof(c_iterator));
        it.has_table_lock = false;
        return this;
    }

    /* release unlocks the table, thereby freeing it up for other
     * operations, but also invalidating all future operations with
     * this iterator. */
    void release() {
        if (has_table_lock) {
            hm_->unlock_all(ti_);
            cuckoohash_map<K, V, H>::unset_hazard_pointer();
            has_table_lock = false;
        }
    }

    ~c_iterator() {
        release();
    }

    /* is_end returns true if the iterator is at end_pos, which means
     * it is past the end of the table. */
    bool is_end() {
        return (index_ == end_pos.first && slot_ == end_pos.second);
    }

    /* is_begin returns true if the iterator is at begin_pos, which
     * means it is at the first item in the table. */
    bool is_begin() {
        return (index_ == begin_pos.first && slot_ == begin_pos.second);
    }


    /* The dereferenc operator returns a copy of the data in the
     * bucket, so that any modification to the returned pair doesn't
     * affect the table itself. Since we don't want to allow the user
     * to arbitrarily modify keys or values, we don't provide a "->"
     * operator. */
    std::pair<K, V> operator*() {
        check_lock();
        if (is_end()) {
            throw std::out_of_range(end_dereference);
        }
        assert(ti_.buckets_[index_].occupied[slot_]);
        return {ti_->buckets_[index_].keys[slot_], ti_->buckets_[index_].vals[slot_]};
    }

    /* ++ moves the iterator forwards to the next nonempty slot. If it
     * reaches the end of the table, it becomes an end iterator. It
     * throws an exception if the iterator is already at the end of
     * the table. */
    c_iterator* operator++() {
        check_lock();
        if (is_end()) {
            throw std::out_of_range(end_increment);
        }
        forward_filled_slot(index_, slot_);
        return this;
    }

    /* This is the same as the prefix version of the operator. */
    c_iterator* operator++(int) {
        check_lock();
        if (is_end()) {
            throw std::out_of_range(end_increment);
        }
        forward_filled_slot(index_, slot_);
        return this;
    }

    /* -- moves the iterator backwards to the previous nonempty slot.
     * If we aren't at the beginning, then the backward_filled_slot
     * operation should not fail. If we are, it throws an
     * exception. */
    c_iterator* operator--() {
        check_lock();
        if (is_begin()) {
            throw std::out_of_range(begin_decrement);
        }
        backward_filled_slot(index_, slot_);
        return this;
    }

    /* This is the same as the prefix version of the operator. */
    c_iterator* operator--(int) {
        check_lock();
        if (is_begin()) {
            throw std::out_of_range(begin_decrement);
        }
        backward_filled_slot(index_, slot_);
        return this;
    }

protected:

    // A pointer to the associated hashmap
    cuckoohash_map<K, V, H> *hm_;

    // The hashmap's table info
    typename cuckoohash_map<K, V, H>::TableInfo *ti_;

    // Indicates whether the iterator has the table lock
    bool has_table_lock;

    // Stores the bucket and slot of the end iterator, which is one
    // past the end of the table. It is initialized during the
    // iterator's constructor.
    std::pair<size_t, size_t> end_pos;

    // Stotres the bucket and slot of the begin iterator, which is the
    // first filled position in the table. It is initialized during
    // the iterator's constructor. If the table is empty, it points
    // past the end of the table, to the same position as end_pos.
    std::pair<size_t, size_t> begin_pos;

    // The bucket index of the item being pointed to
    size_t index_;

    // The slot in the bucket of the item being pointed to
    size_t slot_;

    /* set_end sets the given index and slot to one past the last
     * position in the table. */
    void set_end(size_t& index, size_t& slot) {
        index = hm_->bucket_count();
        slot = 0;
    }

    /* set_begin sets the given pair to the position of the first
     * element in the table. */
    void set_begin(size_t& index, size_t& slot) {
        if (hm_->empty()) {
            set_end(index, slot);
        } else {
            index = slot = 0;
            // There must be a filled slot somewhere in the table
            if (!ti_->buckets_[index].occupied[slot]) {
                forward_filled_slot(index, slot);
                assert(!is_end());
            }
        }
    }

    /* forward_slot moves the given index and slot to the next
     * available slot in the forwards direction. It returns true if it
     * successfully advances, and false if it has reached the end of
     * the table, in which case it sets index and slot to end_pos. */
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

    /* backward_slot moves index and slot to the next available slot
     * in the backwards direction. It returns true if it successfully
     * advances, and false if it has reached the beginning of the
     * table, setting the index and slot back to begin_pos. */
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

    /* forward_filled_slot moves index and slot to the next filled
     * slot. */
    bool forward_filled_slot(size_t& index, size_t& slot) {
        bool res = forward_slot(index, slot);
        if (!res) {
            return false;
        }
        while (!ti_->buckets_[index].occupied[slot]) {
            res = forward_slot(index, slot);
            if (!res) {
                return false;
            }
        }
        return true;
    }

    /* backward_filled_slot moves index and slot to the previous
     * filled slot. */
    bool backward_filled_slot(size_t& index, size_t& slot) {
        bool res = backward_slot(index, slot);
        if (!res) {
            return false;
        }
        while (!ti_->buckets_[index].occupied[slot]) {
            res = backward_slot(index, slot);
            if (!res) {
                return false;
            }
        }
        return true;
    }


    /* check_lock throws an exception if the iterator doesn't have a
     * lock. */
    void check_lock() {
        if (!has_table_lock) {
            throw std::runtime_error("Iterator does not have a lock on the table");
        }
    }

    // Other error messages
    static const char* end_dereference;
    static const char* end_increment;
    static const char* begin_decrement;
};

// Defining static members
template <class K, class V, class H>
const char* c_iterator<K, V, H>::end_dereference = "Cannot dereference: iterator points past the end of the table";
template <class K, class V, class H>
const char* c_iterator<K, V, H>::end_increment = "Cannot increment: iterator points past the end of the table";
template <class K, class V, class H>
const char* c_iterator<K, V, H>::begin_decrement = "Cannot decrement: iterator points to the beginning of the table";

/* A mut_iterator is just a c_iterator with an update method. */
template <class K, class V, class H>
class mut_iterator : public c_iterator<K, V, H> {
public:
    /* These functions do basically the same thing as c_iterators. We
     * also allow creating a mut_iterator from a c_iterator. */
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

    /* set_value sets the value pointed to by the iterator. This
     * involves modifying the hash table itself, but since we have a
     * lock on the table, we are okay. Since we are only changing the
     * value in the bucket, the element will retain it's position, so
     * this is just a simple assignment. */
    void set_value(const V val) {
        this->check_lock();
        if (this->is_end()) {
            throw std::out_of_range(this->end_dereference);
        }
        assert(this->ti_.buckets_[this->index_].occupied[this->slot_]);
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

/* snapshot_table allocates an array and, using a const_iterator
 * stores all the elements currently in the table, returning the
 * array. */
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
