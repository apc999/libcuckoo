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
        Table::const_threadsafe_iterator t = iter_env->emptytable.const_threadsafe_begin();
        ASSERT_TRUE(t.is_begin() && t.is_end());
        t.release();
        t = iter_env->emptytable.const_threadsafe_end();
        ASSERT_TRUE(t.is_begin() && t.is_end());
}

TEST(FilledTable, IterForwards) {
        Table::const_threadsafe_iterator t = iter_env->table.const_threadsafe_begin();
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
}

TEST(FilledTable, IterBackwards) {
        Table::const_threadsafe_iterator t = iter_env->table.const_threadsafe_end();
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
}

int main(int argc, char** argv) {
        iter_env = (IteratorEnvironment*)::testing::AddGlobalTestEnvironment(new IteratorEnvironment);
        ::testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();
}
