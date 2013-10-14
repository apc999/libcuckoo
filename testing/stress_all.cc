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

#include <cstring>
#include <cstdlib>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "gtest/gtest.h"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef std::pair<KeyType, ValType> KVPair;

// The number of keys that can be inserted, deleted, and searched on
const size_t numkeys = 10000000;
// The power argument passed to the hashtable constructor. By default
// it should be enough to hold numkeys elements. This can be set with
// the command line flag --power
size_t power =  22;

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

// When set to true, it signals to the threads to stop running
std::atomic<bool> finished = ATOMIC_VAR_INIT(false);

// A bitfield containing flags for each slot in the array of key-value
// pairs
typedef struct {
    // Whether the slot is in use
    bool in_use : 4;
    // Whether the key-value pair has been inserted into the table
    bool in_table : 4;
} slot_flags_t;

std::atomic<size_t> num_inserts = ATOMIC_VAR_INIT(0);
std::atomic<size_t> num_deletes = ATOMIC_VAR_INIT(0);
std::atomic<size_t> num_finds = ATOMIC_VAR_INIT(0);

class AllEnvironment : public ::testing::Environment {
public:
    AllEnvironment(size_t power = 1)
        : table(power), keys(numkeys), vals(numkeys), flags(numkeys),
          val_dist(std::numeric_limits<ValType>::min(), std::numeric_limits<ValType>::max()),
          ind_dist(0, numkeys-1)
        {}

    virtual void SetUp() {
        // Sets up the random number generator
        uint64_t seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::cout << "seed = " << seed << std::endl;
        gen_seed = seed;

        // Fills in the keys and flags arrays. The vals array will
        // be filled in by the insertion threads.
        for (size_t i = 0; i < numkeys; i++) {
            keys[i] = i;
            flags[i] = slot_flags_t{false, false};
        }
    }

    cuckoohash_map<KeyType, ValType> table;
    std::vector<KeyType> keys;
    std::vector<ValType> vals;
    std::vector<std::atomic<slot_flags_t> > flags;
    std::uniform_int_distribution<ValType> val_dist;
    std::uniform_int_distribution<size_t> ind_dist;
    size_t gen_seed;
};

AllEnvironment* env;

void insert_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Picks a random number between 0 and numkeys. If that slot
        // is not in use and not in the table, it locks the slot,
        // inserts it into the table, checks that the insertion was
        // actually successful with another find operation, and then
        // store the value in the array and set in_table to true
        size_t ind = env->ind_dist(gen);
        slot_flags_t flag_false_false = {false, false};
        if (env->flags[ind].compare_exchange_strong(flag_false_false, slot_flags_t{true, false})) {
            // The slot was not in use and not in the table, so we
            // marked it as in_use and run the insert
            KeyType k = ind;
            ValType v = env->val_dist(gen);
            bool res = env->table.insert(k, v);
            EXPECT_TRUE(res);
            if (res) {
                ValType find_v;
                EXPECT_TRUE(env->table.find(k, find_v));
                EXPECT_EQ(v, find_v);
                // Store v in the array and set in_use to false and in_table to true
                env->vals[ind] = v;
                env->flags[ind].store(slot_flags_t{false, true});
                num_inserts.fetch_add(1, std::memory_order_relaxed);
            } else {
                // Set in_use to false, but leave in_table as false
                env->flags[ind].store(slot_flags_t{false, false});
            }
        }
    }
}

void delete_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Picks a random number between 0 and numkeys. If that slot
        // is not in use and is in the table, it locks the slot, runs
        // a delete on that key, checks that the key is indeed not in
        // the table anymore, and then sets in_table to false
        size_t ind = env->ind_dist(gen);
        slot_flags_t flag_false_true = {false, true};
        if (env->flags[ind].compare_exchange_strong(flag_false_true, slot_flags_t{true, true})) {
            // The slot was not in use and in the table, so we marked
            // it as in_use and run the insert
            KeyType k = ind;
            bool res = env->table.erase(k);
            EXPECT_TRUE(res);
            if (res) {
                ValType find_v;
                EXPECT_FALSE(env->table.find(k, find_v));
                // Sets in_use to false and in_table to false
                env->flags[ind].store(slot_flags_t{false, false});
                num_deletes.fetch_add(1, std::memory_order_relaxed);
            } else {
                // Sets in_use to false but keeps in_table as true
                env->flags[ind].store(slot_flags_t{false, true});
            }
        }
    }
}

void find_thread() {
    std::mt19937_64 gen(env->gen_seed);
    while (!finished.load()) {
        // Picks a random number between 0 and numkeys. If that slot
        // is not in use, it locks the slot, runs a find on that key,
        // and checks that the result is consistent with the in_table
        // value of the flag
        size_t ind = env->ind_dist(gen);
        slot_flags_t ind_flag = env->flags[ind].load();
        if (!ind_flag.in_use && env->flags[ind].compare_exchange_strong(ind_flag, slot_flags_t{true, ind_flag.in_table})) {
            // We marked the slot as in_use, so now we can run the
            // find
            KeyType k = ind;
            ValType v;
            bool res = env->table.find(k, v);
            EXPECT_EQ(ind_flag.in_table, res);
            if (res) {
                EXPECT_EQ(v, env->vals[ind]);
            }
            // Sets in_use to false
            env->flags[ind].store(slot_flags_t{false, ind_flag.in_table});
            num_finds.fetch_add(1, std::memory_order_relaxed);
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
        if (env->flags[i].load().in_table) {
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
    // Checks for the --power, --thread-num, and --time arguments,
    // which are parsed similarly, since they both require a positive
    // integer argument afterwards
    const size_t arg_num = 3;
    std::array<const char*, arg_num> args = {"--power", "--thread-num", "--time"};
    std::array<size_t*, arg_num> arg_vars = {&power, &thread_num, &test_len};
    std::array<const char*, arg_num> arg_help = {"The power argument given to the hashtable during initialization",
                                                 "The number of threads to spawn for each type of operation",
                                                 "The number of seconds to run the test for"};
    const size_t flag_num = 3;
    std::array<const char*, flag_num> flags = {"--disable-inserts", "--disable-deletes", "--disable-finds"};
    std::array<bool*, flag_num> flag_vars = {&disable_inserts, &disable_deletes, &disable_finds};
    std::array<const char*, flag_num> flag_help = {"If set, no inserts will be run",
                                                   "If set, no deletes will be run",
                                                   "If set, no finds will be run"};
    for (int i = 0; i < argc; i++) {
        for (size_t j = 0; j < arg_num; j++) {
            if (strcmp(argv[i], args[j]) == 0) {
                if (i == argc-1) {
                    std::cerr << "You must provide a positive integer argument after the " << args[j] << " argument" << std::endl;
                    exit(1);
                } else {
                    int argval = atoi(argv[i+1]);
                    if (argval <= 0) {
                        std::cerr << "The argument to " << args[j] << " must be a positive integer" << std::endl;
                        exit(1);
                    } else {
                        *(arg_vars[j]) = argval;
                    }
                }
            }
        }
        for (size_t j = 0; j < flag_num; j++) {
            if (strcmp(argv[i], flags[j]) == 0) {
                *(flag_vars[j]) = true;
            }
        }
        if (strcmp(argv[i], "--help") == 0) {
            std::cerr << "Runs a stress test on inserts, deletes, and finds" << std::endl;
            std::cerr << "Arguments:" << std::endl;
            for (size_t j = 0; j < arg_num; j++) {
                std::cerr << args[j] << " (:\t\t" << arg_help[j] << std::endl;
            }
            for (size_t j = 0; j < flag_num; j++) {
                std::cerr << flags[j] << ":\t" << flag_help[j] << std::endl;
            }
            exit(0);
        }
    }

    env = (AllEnvironment*) ::testing::AddGlobalTestEnvironment(new AllEnvironment(power));
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
