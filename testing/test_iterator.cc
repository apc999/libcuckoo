#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <iostream>
#include <cstdint>
#include <algorithm>
#include <utility>
#include <random>
#include <limits>
#include <chrono>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "gtest/gtest.h"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef cuckoohash_map<KeyType, ValType> Table;

const size_t power = 1;
const size_t size = (1L << power) * SLOT_PER_BUCKET;

// Global set up and tear down
class IteratorEnvironment : public ::testing::Environment {
public:
    IteratorEnvironment(): emptytable(power), table(power), items_end(items+size) {}

    void SetUp() {
        // Fills up table and items with random values
        uint64_t seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::cout << "seed = " << seed << std::endl;
        std::uniform_int_distribution<ValType> v_dist(std::numeric_limits<ValType>::min(), std::numeric_limits<ValType>::max());
        std::mt19937_64 gen(seed);
        for (size_t i = 0; i < size; i++) {
            items[i].first = i;
            items[i].second = v_dist(gen);
            EXPECT_TRUE(table.insert(items[i].first, items[i].second));
        }
    }
        
    Table emptytable;
    Table table;
    std::pair<KeyType, ValType> items[size];
    std::pair<KeyType, ValType>* items_end;
};

IteratorEnvironment* iter_env;

TEST(EmptyTable, BeginEndIterator) {
    Table emptytable(power);
    Table::const_iterator t = iter_env->emptytable.cbegin();
    ASSERT_TRUE(t.is_begin() && t.is_end());
    t.release();
    t = iter_env->emptytable.cend();
    ASSERT_TRUE(t.is_begin() && t.is_end());
}

bool check_table_snapshot() {
    Table::value_type *snapshot_items = iter_env->table.snapshot_table();
    for (int i = 0; i < iter_env->table.size(); i++) {
        if (std::find(iter_env->items, iter_env->items_end, snapshot_items[i]) == iter_env->items_end) {
            return false;
        }
    }
    delete[] snapshot_items;
    return true;
}

TEST(FilledTable, IterForwards) {
    Table::const_iterator t = iter_env->table.cbegin();
    bool visited[size] = {};
    while (!t.is_end()) {
        auto itemiter = std::find(iter_env->items, iter_env->items_end, *t);
        EXPECT_NE(itemiter, iter_env->items_end);
        visited[iter_env->items_end-itemiter-1] = true;
        t++;
    }
    // Checks that all the items were visited
    for (size_t i = 0; i < size; i++) {
        EXPECT_TRUE(visited[i]);
    }
    t.release();
    EXPECT_TRUE(check_table_snapshot());
}

TEST(FilledTable, IterBackwards) {
    Table::const_iterator t = iter_env->table.cend();
    bool visited[size] = {};
    do {
        t--;
        auto itemiter = std::find(iter_env->items, iter_env->items_end, *t);
        EXPECT_NE(itemiter, iter_env->items_end);
        visited[iter_env->items_end-itemiter-1] = true;
    } while (!t.is_begin());
    // Checks that all the items were visited
    for (size_t i = 0; i < size; i++) {
        EXPECT_TRUE(visited[i]);
    }
    t.release();
    EXPECT_TRUE(check_table_snapshot());
}

TEST(FilledTable, IncrementItems) {
    for (size_t i = 0; i < size; i++) {
        iter_env->items[i].second++;
    }
    // Also tests casting from a const iterator to a mutable one
    Table::iterator t_mut = static_cast<Table::iterator>(iter_env->table.cbegin());
    Table::value_type p;
    while (!t_mut.is_end()) {
        p = *t_mut;
        t_mut.set_value(p.second+1);
        EXPECT_NE(std::find(iter_env->items, iter_env->items_end, *t_mut), iter_env->items_end);
        t_mut++;
    }
}

int main(int argc, char** argv) {
    iter_env = (IteratorEnvironment*)::testing::AddGlobalTestEnvironment(new IteratorEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
