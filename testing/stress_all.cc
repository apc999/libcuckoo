/* Tests concurrent inserts, deletes, and finds. The test makes sure
 * that multiple operations are not run on the same key, so that the
 * accuracy of the operations can be verified */
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
#include <mutex>
#include <array>
#include <vector>
#include <atomic>
#include <thread>

#include "commandline_parser.cc"
#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "gtest/gtest.h"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef std::pair<KeyType, ValType> KVPair;

// The number of keys that can be inserted, deleted, and searched on
const size_t numkeys = 100000000L;
// The power argument passed to the hashtable constructor. By default
// it should be enough to hold numkeys elements. This can be set with
// the command line flag --power
size_t power =  24;

// The number of threads spawned for each type of operation. This can
// be set with the command line flag --thread-num
size_t thread_num = 4;
// Whether to disable inserts or not. This can be set with the command
// line flag --disable-inserts
bool disable_inserts = false;
// Whether to disable deletes or not. This can be set with the command
// line flag --disable-deletes
bool disable_deletes = false;
// Whether to disable finds or not. This can be set with the command
// line flag --disable-finds
bool disable_finds = false;
// How many seconds to run the test for. This can be set with the
// command line flag --time
size_t test_len = 10;
// The seed for the random number generator. If this isn't set to a
// nonzero value with the --seed flag, the current time is used
size_t seed = 0;

// When set to true, it signals to the threads to stop running
std::atomic<bool> finished = ATOMIC_VAR_INIT(false);

std::atomic<size_t> num_inserts = ATOMIC_VAR_INIT(0);
std::atomic<size_t> num_deletes = ATOMIC_VAR_INIT(0);
std::atomic<size_t> num_finds = ATOMIC_VAR_INIT(0);

class AllEnvironment : public ::testing::Environment {
public:
    AllEnvironment()
        : table(power), keys(numkeys), vals(numkeys), in_table(new bool[numkeys]), in_use(numkeys),
          val_dist(std::numeric_limits<ValType>::min(), std::numeric_limits<ValType>::max()),
          ind_dist(0, numkeys-1)
        {}

    virtual void SetUp() {
        // Sets up the random number generator
        if (seed == 0) {
            seed = std::chrono::system_clock::now().time_since_epoch().count();
        }
        std::cout << "seed = " << seed << std::endl;
        gen_seed = seed;

        // Fills in all the vectors except vals, which will be filled
        // in by the insertion threads.
        for (size_t i = 0; i < numkeys; i++) {
            keys[i] = i;
            in_table[i] = false;
            in_use[i].clear();
        }
    }

    cuckoohash_map<KeyType, ValType> table;
    std::vector<KeyType> keys;
    std::vector<ValType> vals;
    std::unique_ptr<bool[]> in_table;
    std::vector<std::atomic_flag> in_use;
    std::uniform_int_distribution<ValType> val_dist;
    std::uniform_int_distribution<size_t> ind_dist;
    size_t gen_seed;
};

AllEnvironment* env;

void insert_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Pick a random number between 0 and numkeys. If that slot is
        // not in use, lock the slot. If the value is already in the
        // table, clear in_use and continue. Otherwise, insert a
        // random value into the table, check that the insertion was
        // actually successful with another find operation, and then
        // store the value in the array and set in_table to true and
        // clear in_use
        size_t ind = env->ind_dist(gen);
        if (!env->in_use[ind].test_and_set()) {
            if (env->in_table[ind] == false) {
                KeyType k = ind;
                ValType v = env->val_dist(gen);
                bool res = env->table.insert(k, v);
                EXPECT_TRUE(res);
                if (res) {
                    ValType find_v;
                    EXPECT_TRUE(env->table.find(k, find_v));
                    EXPECT_EQ(v, find_v);
                    env->vals[ind] = v;
                    env->in_table[ind] = true;
                    num_inserts.fetch_add(1, std::memory_order_relaxed);
                }
            }
            env->in_use[ind].clear();
        }
    }
}

void delete_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Run a delete on a random key that is in the table, check
        // that the key is indeed not in the table anymore, and then
        // set in_table to false
        size_t ind = env->ind_dist(gen);
        if (!env->in_use[ind].test_and_set()) {
            if (env->in_table[ind] == true) {
                KeyType k = ind;
                bool res = env->table.erase(k);
                EXPECT_TRUE(res);
                if (res) {
                    ValType find_v;
                    res = env->table.find(k, find_v);
                    EXPECT_FALSE(res);
                    env->in_table[ind] = false;
                    num_deletes.fetch_add(1, std::memory_order_relaxed);
                }
            }
            env->in_use[ind].clear();
        }
    }
}

void find_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Run a find on a random key and check that the presence of
        // the key matches in_table
        size_t ind = env->ind_dist(gen);
        bool expected = false;
        if (!env->in_use[ind].test_and_set()) {
            KeyType k = ind;
            ValType v;
            bool res = env->table.find(k, v);
            EXPECT_EQ(env->in_table[ind], res);
            if (res) {
                EXPECT_EQ(v, env->vals[ind]);
            }
            num_finds.fetch_add(1, std::memory_order_relaxed);
            env->in_use[ind].clear();
        }
    }
}

// Spawns thread_num insert, delete, and find threads
TEST(AllTest, Everything) {
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_num; i++) {
        if (!disable_inserts) {
            threads.emplace_back(insert_thread);
        }
        if (!disable_deletes) {
            threads.emplace_back(delete_thread);
        }
        if (!disable_finds) {
            threads.emplace_back(find_thread);
        }
    }
    // Sleeps before ending the threads
    std::this_thread::sleep_for(std::chrono::milliseconds(test_len * 1000));
    finished.store(true);
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    // Finds the number of slots that are filled
    size_t numfilled = 0;
    for (size_t i = 0; i < numkeys; i++) {
        if (env->in_table[i]) {
            numfilled++;
        }
    }
    EXPECT_EQ(numfilled, env->table.size());
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Number of inserts:\t" << num_inserts.load() << std::endl;
    std::cout << "Number of deletes:\t" << num_deletes.load() << std::endl;
    std::cout << "Number of finds:\t" << num_finds.load() << std::endl;
    std::cout << "Table report:" << std::endl;
    env->table.report();
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--thread-num", "--time", "--seed"};
    size_t* arg_vars[] = {&power, &thread_num, &test_len, &seed};
    const char* arg_help[] = {"The power argument given to the hashtable during initialization",
                              "The number of threads to spawn for each type of operation",
                              "The number of seconds to run the test for",
                              "The seed for the random number generator"};
    const char* flags[] = {"--disable-inserts", "--disable-deletes", "--disable-finds"};
    bool* flag_vars[] = {&disable_inserts, &disable_deletes, &disable_finds};
    const char* flag_help[] = {"If set, no inserts will be run",
                               "If set, no deletes will be run",
                               "If set, no finds will be run"};
    parse_flags(argc, argv, args, arg_vars, arg_help, 4, flags, flag_vars, flag_help, 3);

    env = (AllEnvironment*) ::testing::AddGlobalTestEnvironment(new AllEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
