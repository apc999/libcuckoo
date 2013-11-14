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
#include <stdint.h>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "test_util.cc"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef int32_t ValType2;
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
// Whether to disable updates or not. This can be set with the command
// line flag --disable-updates
bool disable_updates = false;
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
std::atomic<size_t> num_updates = ATOMIC_VAR_INIT(0);
std::atomic<size_t> num_finds = ATOMIC_VAR_INIT(0);

class AllEnvironment {
public:
    AllEnvironment()
        : table(power), table2(power), keys(numkeys), vals(numkeys), vals2(numkeys), in_table(new bool[numkeys]), in_use(numkeys),
          val_dist(std::numeric_limits<ValType>::min(), std::numeric_limits<ValType>::max()),
          val_dist2(std::numeric_limits<ValType2>::min(), std::numeric_limits<ValType2>::max()),
          ind_dist(0, numkeys-1) {
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
    cuckoohash_map<KeyType, ValType2> table2;
    std::vector<KeyType> keys;
    std::vector<ValType> vals;
    std::vector<ValType2> vals2;
    std::unique_ptr<bool[]> in_table;
    std::vector<std::atomic_flag> in_use;
    std::uniform_int_distribution<ValType> val_dist;
    std::uniform_int_distribution<ValType2> val_dist2;
    std::uniform_int_distribution<size_t> ind_dist;
    size_t gen_seed;
};

AllEnvironment* env;

void insert_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Pick a random number between 0 and numkeys. If that slot is
        // not in use, lock the slot. Insert a random value into both
        // tables. The inserts should only be successful if the key
        // wasn't in the table. If the inserts succeeded, check that
        // the insertion were actually successful with another find
        // operation, and then store the values in their arrays and
        // set in_table to true and clear in_use
        size_t ind = env->ind_dist(gen);
        if (!env->in_use[ind].test_and_set()) {
            KeyType k = ind;
            ValType v = env->val_dist(gen);
            ValType2 v2 = env->val_dist2(gen);
            bool res = env->table.insert(k, v);
            bool res2 = env->table2.insert(k, v2);
            EXPECT_NE(res, env->in_table[ind]);
            EXPECT_NE(res2, env->in_table[ind]);
            if (res) {
                ValType find_v = 0;
                ValType2 find_v2 = 0;
                EXPECT_TRUE(env->table.find(k, find_v));
                EXPECT_EQ(v, find_v);
                EXPECT_TRUE(env->table2.find(k, find_v2));
                EXPECT_EQ(v2, find_v2);
                env->vals[ind] = v;
                env->vals2[ind] = v2;
                env->in_table[ind] = true;
                num_inserts.fetch_add(2, std::memory_order_relaxed);
            }
            env->in_use[ind].clear();
        }
    }
}

void delete_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Run deletes on a random key, check that the deletes
        // succeeded only if the keys were in the table. If the
        // deletes succeeded, check that the keys are indeed not in
        // the tables anymore, and then set in_table to false
        size_t ind = env->ind_dist(gen);
        if (!env->in_use[ind].test_and_set()) {
            KeyType k = ind;
            bool res = env->table.erase(k);
            bool res2 = env->table2.erase(k);
            EXPECT_EQ(res, env->in_table[ind]);
            EXPECT_EQ(res2, env->in_table[ind]);
            if (res) {
                ValType find_v = 0;
                ValType2 find_v2 = 0;
                EXPECT_FALSE(env->table.find(k, find_v));
                EXPECT_FALSE(env->table2.find(k, find_v2));
                env->in_table[ind] = false;
                num_deletes.fetch_add(2, std::memory_order_relaxed);
            }
            env->in_use[ind].clear();
        }
    }
}

void update_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Run updates on a random key, check that the updates
        // succeeded only if the keys were in the table. If the
        // updates succeeded, check that the keys are indeed in the
        // table with the new value, and then set in_table to true
        size_t ind = env->ind_dist(gen);
        if (!env->in_use[ind].test_and_set()) {
            KeyType k = ind;
            ValType v = env->val_dist(gen);
            ValType2 v2 = env->val_dist2(gen);
            bool res = env->table.update(k, v);
            bool res2 = env->table2.update(k, v2);
            EXPECT_EQ(res, env->in_table[ind]);
            EXPECT_EQ(res2, env->in_table[ind]);
            if (res) {
                ValType find_v = 0;
                ValType2 find_v2 = 0;
                EXPECT_TRUE(env->table.find(k, find_v));
                EXPECT_EQ(v, find_v);
                EXPECT_TRUE(env->table2.find(k, find_v2));
                EXPECT_EQ(v2, find_v2);
                env->vals[ind] = v;
                env->vals2[ind] = v2;
                num_updates.fetch_add(2, std::memory_order_relaxed);
            }
            env->in_use[ind].clear();
        }
    }
}


void find_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Run finds on a random key and check that the presence of
        // the keys matches in_table
        size_t ind = env->ind_dist(gen);
        if (!env->in_use[ind].test_and_set()) {
            KeyType k = ind;
            ValType v = 0;
            ValType2 v2 = 0;
            bool res = env->table.find(k, v);
            bool res2 = env->table2.find(k, v2);
            EXPECT_EQ(env->in_table[ind], res);
            EXPECT_EQ(env->in_table[ind], res2);
            if (res) {
                EXPECT_EQ(v, env->vals[ind]);
                EXPECT_EQ(v2, env->vals2[ind]);
            }
            num_finds.fetch_add(2, std::memory_order_relaxed);
            env->in_use[ind].clear();
        }
    }
}

// Spawns thread_num insert, delete, update, and find threads
void StressTest() {
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_num; i++) {
        if (!disable_inserts) {
            threads.emplace_back(insert_thread);
        }
        if (!disable_deletes) {
            threads.emplace_back(delete_thread);
        }
        if (!disable_updates) {
            threads.emplace_back(update_thread);
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
    std::cout << "Number of updates:\t" << num_updates.load() << std::endl;
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
    const char* flags[] = {"--disable-inserts", "--disable-deletes", "--disable-updates", "--disable-finds"};
    bool* flag_vars[] = {&disable_inserts, &disable_deletes, &disable_updates, &disable_finds};
    const char* flag_help[] = {"If set, no inserts will be run",
                               "If set, no deletes will be run",
                               "If set, no updates will be run",
                               "If set, no finds will be run"};
    parse_flags(argc, argv, args, arg_vars, arg_help, sizeof(args)/sizeof(const char*), flags, flag_vars, flag_help, sizeof(flags)/sizeof(const char*));

    env = new AllEnvironment;
    StressTest();
}
