#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <iostream>
#include <algorithm>
#include <utility>
#include <memory>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "gtest/gtest.h"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef std::pair<KeyType, ValType> KVPair;

size_t power = 19;
size_t numkeys = (1 << power) * SLOT_PER_BUCKET;

TEST(Insert, SmallAndBig) {
    // Initializing the hash tables
    cuckoohash_map<KeyType, ValType> smalltable(power);
    cuckoohash_map<KeyType, ValType> bigtable(power + 1);

    // Inserting elements into the table
    for (size_t i = 1; i < numkeys; i++) {
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;

        EXPECT_TRUE(smalltable.insert(key, val));
        EXPECT_TRUE(bigtable.insert(key, val));
    }

    // Comparing the tables
    ASSERT_EQ(smalltable.size(), bigtable.size());

    std::unique_ptr<KVPair[]> smallElems(smalltable.snapshot_table());
    std::unique_ptr<KVPair[]> bigElems(bigtable.snapshot_table());
    std::sort(smallElems.get(), smallElems.get()+smalltable.size());
    std::sort(bigElems.get(), bigElems.get()+bigtable.size());

    for (size_t i = 0; i < smalltable.size(); i++) {
        EXPECT_TRUE(smallElems[i] == bigElems[i]);
    }
}
