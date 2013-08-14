#ifndef _CUCKOOHASH_MAP_H
#define _CUCKOOHASH_MAP_H

#include <utility>

extern "C" {
#include "cuckoohash.h"
#include "cuckoohash_config.h"
}

using namespace std;

//
// Forward declaration
//
template <typename KeyType, typename ValType>
class cuckoohash_map;


template <typename KeyType, typename ValType> 
struct cuckoohash_map_iterator;


// not supported yet 
//template <typename KeyType, typename ValType> 
//struct cuckoohash_map_const_iterator;


template <typename KeyType, typename ValType> 
struct cuckoohash_map_iterator  {

public:
    typedef cuckoohash_map_iterator<KeyType, ValType> iterator;
    typedef std::pair<KeyType, ValType> value_type;


    cuckoohash_map_iterator(const cuckoohash_map<KeyType, ValType> *h,
                            const KeyType& k,
                            const ValType& v,
                            const bool     is_end)
    : ht(h), key(k), val(v), is_end(is_end), data(value_type(k, v)) {
    }

    cuckoohash_map_iterator(const iterator& it)
    : ht(it.ht), key(it.key), val(it.val), is_end(it.is_end), data(it.data) {
    }

    cuckoohash_map_iterator()
        : ht(NULL), is_end(true) { 
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
    const cuckoohash_map<KeyType, ValType> *ht;
    KeyType    key;
    ValType    val;
    bool       is_end;
    value_type data;
};


template <typename KeyType, typename ValType> 
class cuckoohash_map {
private:
    cuckoo_hashtable_t *ht;

public:

    typedef cuckoohash_map_iterator<KeyType, ValType> iterator;
    typedef size_t size_type;

    //
    // Constructor
    //
    explicit
    cuckoohash_map(size_t hashpower_init = HASHPOWER_DEFAULT) 
        : ht(cuckoo_init(hashpower_init, sizeof(KeyType), sizeof(ValType))) {
    }

    void clear() {
    }

    //
    // Iterator functions
    //
    iterator begin() {
        assert(false);
        return iterator();
    }

    iterator end() {
        return iterator(this, (KeyType) 0, (ValType) 0, true);
    }

    
    //
    // Functions concerning size
    //
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


    //
    float load_factor() const {
        return cuckoo_loadfactor(ht);
    }
    
    float max_load_factor() const {
        return 1.0;
    }

    
    //
    // Lookup routines
    //
    iterator find(const KeyType& key) {
        cuckoo_status st;
        ValType val;

        st = cuckoo_find(ht, (const char*) &key, (char*) &val);
        if (st == ok) {
            return iterator(this, key, val, false);
        } else {
            return end();
        }
    }

    //
    // If key exists, return the 
    //
    //ValType& operator[](const KeyType& key) {
    //}

    //
    // Insertion routines
    //
    bool insert(const KeyType& key, const ValType& val) {
        cuckoo_status st;

        st = cuckoo_insert(ht, (const char*) &key, (char*) &val);
        if (st == ok) {
            return true;
        } else {
            return false;
        }
        
    }

    //
    // Deletion routines
    //
    size_type erase(const KeyType& key) {
        cuckoo_status st;
        
        st = cuckoo_delete(ht, (const char*) &key);
        if (st == ok) {
            return 1;
        } else {
            return false;
        }
    }

    //void erase(iterator it) {
    //}

};

#endif
