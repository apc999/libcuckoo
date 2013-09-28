/* A simple example of iterating through a cuckoo hash table with a
 * threadsafe iterator */

#include <iostream>
#include <cstdint>
#include <algorithm>

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
        Table::threadsafe_iterator t = table.threadsafe_begin();
        check_cond(t.is_begin() && t.is_end(), "Begin iterator on empty table is not beginning and end");

        std::cout << "Move assignment to end iterator" << std::endl;
        t.release();
        t = table.threadsafe_end();
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
        t = table.threadsafe_begin();
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

        std::cout << "Incrementing the values of each element in the table" << std::endl;
        for (size_t i = 0; i < size; i++) {
                items[i].second++;
        }
        while (!t.is_end()) {
                p = *t;
                t.set_value(p.second+1);
                check_item(items, items+size, *t);
                t++;
        }

        if (failures == 0) {
                std::cout << "[PASSED]" << std::endl;
        } else {
                std::cout << "[FAILED with " << failures << " failures]" << std::endl;
        }
}
