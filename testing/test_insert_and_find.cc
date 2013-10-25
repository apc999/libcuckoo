#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <iostream>
#include <algorithm>
#include <utility>
#include <memory>
#include <random>
#include <limits>
#include <chrono>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "gtest/gtest.h"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef std::pair<KeyType, ValType> KVPair;

const size_t power = 19;
const size_t numkeys = (1 << power) * SLOT_PER_BUCKET;

class InsertFindTest : public ::testing::Test {
protected:
    
    InsertFindTest()
        : smalltable(power), bigtable(power + 1) {}

    virtual void SetUp() {
        // Sets up the random number generator
        uint64_t seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::cout << "seed = " << seed << std::endl;
        std::uniform_int_distribution<ValType> v_dist(std::numeric_limits<ValType>::min(), std::numeric_limits<ValType>::max());
        std::mt19937_64 gen(seed);

        // Inserting elements into the table
        for (size_t i = 0; i < numkeys; i++) {
            keys[i] = i;
            vals[i] = v_dist(gen);
            EXPECT_TRUE(smalltable.insert(keys[i], vals[i]));
            EXPECT_TRUE(bigtable.insert(keys[i], vals[i]));
        }
        // Fills up nonkeys with keys that aren't in the table
        std::uniform_int_distribution<KeyType> k_dist(std::numeric_limits<KeyType>::min(), std::numeric_limits<KeyType>::max());
        for (size_t i = 0; i < numkeys; i++) {
            KeyType k;
            do {
                k = k_dist(gen);
            } while (k >= 0 && k < numkeys);
            nonkeys[i] = k;
        }
    }

    cuckoohash_map<KeyType, ValType> smalltable;
    cuckoohash_map<KeyType, ValType> bigtable;
    KeyType keys[numkeys];
    ValType vals[numkeys];
    KeyType nonkeys[numkeys];
};

// Makes sure that we can find all the keys with their matching values
// in the small and big tables
TEST_F(InsertFindTest, FindKeysInTables) {
    ASSERT_EQ(smalltable.size(), numkeys);
    ASSERT_EQ(bigtable.size(), numkeys);

    ValType retval;
    for (size_t i = 0; i < numkeys; i++) {
        EXPECT_TRUE(smalltable.find(keys[i], retval));
        EXPECT_EQ(retval, vals[i]);
        EXPECT_TRUE(bigtable.find(keys[i], retval));
        EXPECT_EQ(retval, vals[i]);
    }
}

// Makes sure than none of the nonkeys are in either table
TEST_F(InsertFindTest, FindNonkeysInTables) {
    ValType retval;
    for (size_t i = 0; i < numkeys; i++) {
        EXPECT_FALSE(smalltable.find(nonkeys[i], retval));
        EXPECT_FALSE(bigtable.find(nonkeys[i], retval));
    }
}
