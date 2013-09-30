/* A simple example of iterating through and modifying a cuckoo hash
 * table with the various iterator and snapshot features */

#include <iostream>
#include <cstdint>
#include <algorithm>
#include <utility>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET

typedef uint32_t KeyType;
typedef uint32_t ValueType;
typedef cuckoohash_map<KeyType, ValueType> Table;

const size_t power = 1;
const size_t size = (1L << power) * SLOT_PER_BUCKET;

size_t failures = 0;

void check_cond(bool cond, const char* errmsg) {
        if (!cond) {
                failures++;
                std::cerr << errmsg << std::endl;
        }
}

template <class InputIterator>
void check_item(InputIterator start, InputIterator end, std::pair<KeyType, ValueType> elm) {
        if (std::search_n(start, end, 1, elm) == end) {
                failures++;
                std::cerr << "Item (" << elm.first << ", " << elm.second << ") not found in table" << std::endl;
        }
}

int main() {
        Table table(power);

        std::cout << "Begin iterator" << std::endl;
        Table::const_threadsafe_iterator t = table.const_threadsafe_begin();
        check_cond(t.is_begin() && t.is_end(), "Begin iterator on empty table is not beginning and end");

        std::cout << "Move assignment to end iterator" << std::endl;
        t.release();
        t = table.const_threadsafe_end();
        check_cond(t.is_begin() && t.is_end(), "End iterator on empty table is not beginning and end");
        t.release();

        std::cout << "Filling up the table" << std::endl;
        std::pair<KeyType, ValueType> items[size];
        for (size_t i = 0; i < size; i++) {
                items[i].first = i;
                items[i].second = i*2+1;
                if (!table.insert(items[i].first, items[i].second)) {
                        failures++;
                        std::cerr << "Insertion of (" << items[i].first << ", " << items[i].second << ") failed" << std::endl;
                }
        }

        std::pair<KeyType, ValueType> p;
        std::cout << "Iterating forwards through the table" << std::endl;
        t = table.const_threadsafe_begin();
        while (!t.is_end()) {
                p = *t;
                check_item(items, items+size, p);
                t++;
        }

        std::cout << "Iterating backwards through the table" << std::endl;
        t--;
        while (!t.is_begin()) {
                p = *t;
                check_item(items, items+size, p);
                t--;
        }
        p = *t;

        std::cout << "Checking table snapshot" << std::endl;
        t.release();
        Table::value_type *snapshot_items = table.snapshot_table();
        for (int i = 0; i < table.size(); i++) {
                check_item(items, items+size, snapshot_items[i]);
        }
        delete[] snapshot_items;

        std::cout << "Incrementing the values of each element in the table" << std::endl;
        for (size_t i = 0; i < size; i++) {
                items[i].second++;
        }
        // Also tests casting from a const iterator to a mutable one
        Table::threadsafe_iterator t_mut = static_cast<Table::threadsafe_iterator>(table.const_threadsafe_begin());
        while (!t_mut.is_end()) {
                p = *t_mut;
                t_mut.set_value(p.second+1);
                check_item(items, items+size, *t_mut);
                t_mut++;
        }

        std::cout << "Checking table snapshot" << std::endl;
        t_mut.release();
        snapshot_items = table.snapshot_table();
        for (int i = 0; i < table.size(); i++) {
                check_item(items, items+size, snapshot_items[i]);
        }
        delete[] snapshot_items;


        if (failures == 0) {
                std::cout << "[PASSED]" << std::endl;
        } else {
                std::cout << "[FAILED with " << failures << " failures]" << std::endl;
        }
}
