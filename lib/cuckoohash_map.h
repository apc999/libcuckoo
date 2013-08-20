#ifndef _CUCKOOHASH_MAP_H
#define _CUCKOOHASH_MAP_H

#include <utility>

#include "cuckoohash.h"
#include "cuckoohash_config.h"

// Forward declaration
template <typename Key, typename T>
class cuckoohash_map;


template <typename Key, typename T>
struct cuckoohash_map_iterator;


template <typename Key, typename T>
struct cuckoohash_map_iterator  {

public:
    typedef Key key_type;
    typedef std::pair<Key, T> value_type;
    typedef T mapped_type;

    typedef cuckoohash_map_iterator<key_type, mapped_type> iterator;

    cuckoohash_map_iterator(const cuckoohash_map<key_type, mapped_type> *h,
                            const key_type& k,
                            const mapped_type& v)
    : ht(h), key(k), val(v), data(value_type(k, v)), is_end(false) {
    }

    cuckoohash_map_iterator(const iterator& it)
    : ht(it.ht), key(it.key), val(it.val), is_end(it.is_end), data(it.data) {
    }

    cuckoohash_map_iterator(const cuckoohash_map<key_type, mapped_type> *h,
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
    const cuckoohash_map<key_type, mapped_type> *ht;
    key_type    key;
    mapped_type val;
    bool        is_end;
    value_type  data;
};


template <typename Key, typename T>
class cuckoohash_map {
private:
    cuckoo_hashtable_t *ht;

public:

    // Type definitions
    typedef Key key_type;
    typedef std::pair<Key, T> value_type;
    typedef T mapped_type;

    typedef cuckoohash_map_iterator<Key, T> iterator;

    typedef size_t size_type;

    // Constructor and destructor
    explicit
    cuckoohash_map(size_t hashpower_init = HASHPOWER_DEFAULT)
        : ht(cuckoo_init(hashpower_init, sizeof(key_type), sizeof(mapped_type))) {
    }

    ~cuckoohash_map() {
        cuckoo_exit(ht);
    }

    void clear() {
        cuckoo_clear(ht);
    }

    // Iterator functions
    iterator begin() {
        assert(false);
        return iterator(ht);
    }

    iterator end() {
        return iterator(this, true);
    }


    // Functions concerning size
    size_type size() const {
        return (ht->hashitems);
    }

    size_type max_size() const {
        assert(false);
        return 0;
    }

    bool empty() const {
        return (ht->hashitems == 0);
    }

    size_type bucket_count() const {
        return (1 << ht->hashpower);
    }

    size_type max_bucket_count() const {
        assert(false);
        return 0;
    }


    // Load factor
    float load_factor() const {
        return cuckoo_loadfactor(ht);
    }

    float max_load_factor() const {
        return 1.0;
    }


    // Lookup routines
    bool find(const key_type& key, mapped_type& val) {
        cuckoo_status st;

        st = cuckoo_find(ht, (const char*) &key, (char*) &val);
        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    iterator find(const key_type& key) {
        cuckoo_status st;
        mapped_type   val;

        st = cuckoo_find(ht, (const char*) &key, (char*) &val);
        if (st == ok) {
            return iterator(this, key, val);
        } else {
            return end();
        }
    }

    // Insertion routines
    bool insert(const key_type& key, const mapped_type& val) {
        cuckoo_status st;

        st = cuckoo_insert(ht, (const char*) &key, (char*) &val);
        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    // Deletion routines
    bool erase(const key_type& key) {
        cuckoo_status st;

        st = cuckoo_delete(ht, (const char*) &key);
        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    // None standard functions:
    // Update routines
    bool update(const key_type& key, const mapped_type& val) {
        cuckoo_status st;

        st = cuckoo_update(ht, (const char*) &key, (const char*) val);
        if (st == ok) {
            return true;
        } else {
            return false;
        }
    }

    void report() {
        cuckoo_report(ht);
    }
};

#endif
