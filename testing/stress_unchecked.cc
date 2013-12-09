/* Tests all operations and iterators concurrently. It doesn't check
 * any operation for correctness, only making sure that everything
 * completes without crashing. */

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
#include <unistd.h>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "test_util.cc"

typedef uint32_t KeyType;
typedef std::string KeyType2;
typedef uint32_t ValType;
typedef int32_t ValType2;

// The number of keys that can be inserted, deleted, and searched on.
// For expensive keys, like strings, it can take a while to generate
// keys, so we define numkeys to be 2^max(20, power) *
// SLOT_PER_BUCKET.
size_t numkeys;
// The power argument passed to the hashtable constructor. By default
// it should be enough to hold numkeys elements. This can be set with
// the command line flag --power
size_t power = 21;
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
// Whether to disable resizes operations or not. This can be set with
// the command line flag --disable-resizes
bool disable_resizes = false;
// Whether to disable iterator operations or not. This can be set with
// the command line flag --disable-iterators
bool disable_iterators = false;
// Whether to disable statistic operations or not. This can be set with
// the command line flag --disable-statistics
bool disable_statistics = false;
// Whether to disable clear operations or not. This can be set with
// the command line flag --disable-clears
bool disable_clears = false;
// How many seconds to run the test for. This can be set with the
// command line flag --time
size_t test_len = 10;
// The seed for the random number generator. If this isn't set to a
// nonzero value with the --seed flag, the current time is used
size_t seed = 0;
// Whether to use strings as the key
bool use_strings = false;

template <class KType>
class AllEnvironment {
public:
    AllEnvironment()
        : table(power), table2(power), keys(numkeys), vals(numkeys), vals2(numkeys),
          val_dist(std::numeric_limits<ValType>::min(), std::numeric_limits<ValType>::max()),
          val_dist2(std::numeric_limits<ValType2>::min(), std::numeric_limits<ValType2>::max()),
          ind_dist(0, numkeys-1), finished(false) {
        // Sets up the random number generator
        if (seed == 0) {
            seed = std::chrono::system_clock::now().time_since_epoch().count();
        }
        std::cout << "seed = " << seed << std::endl;
        gen_seed = seed;

        // Fills in all the vectors except vals, which will be filled
        // in by the insertion threads.
        for (size_t i = 0; i < numkeys; i++) {
            keys[i] = generateKey<KType>(i);
        }
    }

    cuckoohash_map<KType, ValType> table;
    cuckoohash_map<KType, ValType2> table2;
    std::vector<KType> keys;
    std::vector<ValType> vals;
    std::vector<ValType2> vals2;
    std::uniform_int_distribution<ValType> val_dist;
    std::uniform_int_distribution<ValType2> val_dist2;
    std::uniform_int_distribution<size_t> ind_dist;
    size_t gen_seed;
    std::atomic<bool> finished;
};

template <class KType>
void insert_thread(AllEnvironment<KType> *env, size_t seed) {
    std::mt19937_64 gen(seed);
    while (!env->finished.load()) {
        // Pick a random number between 0 and numkeys and insert a
        // random value into the table on that key.
        size_t ind = env->ind_dist(gen);
        KType k = env->keys[ind];
        ValType v = env->val_dist(gen);
        env->table.insert(env->keys[ind], v);
        env->table2.insert(k, env->val_dist2(gen));
    }
}

template <class KType>
void delete_thread(AllEnvironment<KType> *env, size_t seed) {
    std::mt19937_64 gen(seed);
    while (!env->finished.load()) {
        // Run deletes on a random key.
        size_t ind = env->ind_dist(gen);
        const KType k = env->keys[ind];
        env->table.erase(env->keys[ind]);
        env->table2.erase(k);
    }
}

template <class KType>
void update_thread(AllEnvironment<KType> *env, size_t seed) {
    std::mt19937_64 gen(seed);
    while (!env->finished.load()) {
        // Run updates on a random key.
        size_t ind = env->ind_dist(gen);
        KType k = env->keys[ind];
        ValType v2 = env->val_dist2(gen);
        env->table.update(k, env->val_dist(gen));
        env->table2.update(env->keys[ind], v2);
    }
}

template <class KType>
void find_thread(AllEnvironment<KType> *env, size_t seed) {
    std::mt19937_64 gen(seed);
    while (!env->finished.load()) {
        // Run finds on a random key.
        size_t ind = env->ind_dist(gen);
        KType k = env->keys[ind];
        ValType v = 0;
        env->table.find(k, v);
        try {
            env->table2.find(env->keys[ind]);
        } catch (...) {}
    }
}

template <class KType>
void resize_thread(AllEnvironment<KType> *env, size_t seed) {
    std::mt19937_64 gen(seed);
    // Resizes between 3 and 5 times
    const size_t num_resizes = gen() % 3 + 3;
    const size_t sleep_time = test_len / num_resizes;
    for (size_t i = 0; i < num_resizes; i++) {
        if (env->finished.load()) {
            return;
        }
        sleep(sleep_time);
        if (env->finished.load()) {
            return;
        }
        const size_t hashpower = env->table2.hashpower();
        if (gen() & 1) {
            env->table.rehash(hashpower + 1);
            env->table.rehash(hashpower / 2);
        } else {
            env->table2.reserve((1U << (hashpower+1)) * SLOT_PER_BUCKET);
            env->table2.reserve((1U << hashpower) * SLOT_PER_BUCKET);
        }
    }
}

template <class KType>
void iterator_thread(AllEnvironment<KType> *env, size_t seed) {
    std::mt19937_64 gen(seed);
    // Runs iteration operations between 5 and 8 times
    const size_t num_resizes = gen() % 4 + 5;
    const size_t sleep_time = test_len / num_resizes;
    for (size_t i = 0; i < num_resizes; i++) {
        if (env->finished.load()) {
            return;
        }
        sleep(sleep_time);
        if (env->finished.load()) {
            return;
        }
        if (gen() & 1) {
             for (auto it = env->table2.begin(); !it.is_end(); it++) {
                if (gen() & 1) {
                    it.set_value((*it).second + 1);
                }
            }
        } else {
            auto res = env->table.snapshot_table();
        }
    }
}

template <class KType>
void statistics_thread(AllEnvironment<KType> *env) {
    // Runs all the statistics functions
    std::mt19937_64 gen(seed);
    if (env->finished.load()) {
        env->table.size();
        env->table.empty();
        env->table.bucket_count();
        env->table.load_factor();
    }
}

template <class KType>
void clear_thread(AllEnvironment<KType> *env, size_t seed) {
    std::mt19937_64 gen(seed);
    // Runs clear operations between 3 and 5 times
    const size_t num_resizes = gen() % 3 + 3;
    const size_t sleep_time = test_len / num_resizes;
    for (size_t i = 0; i < num_resizes; i++) {
        if (env->finished.load()) {
            return;
        }
        sleep(sleep_time);
        if (env->finished.load()) {
            return;
        }
        env->table.clear();
    }
}

// Spawns thread_num threads for each type of operation
template <class KType>
void StressTest(AllEnvironment<KType> *env) {
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_num; i++) {
        if (!disable_inserts) {
            threads.emplace_back(insert_thread<KType>, env, env->gen_seed++);
        }
        if (!disable_deletes) {
            threads.emplace_back(delete_thread<KType>, env, env->gen_seed++);
        }
        if (!disable_updates) {
            threads.emplace_back(update_thread<KType>, env, env->gen_seed++);
        }
        if (!disable_finds) {
            threads.emplace_back(find_thread<KType>, env, env->gen_seed++);
        }
        if (!disable_resizes) {
            threads.emplace_back(resize_thread<KType>, env, env->gen_seed++);
        }
        if (!disable_iterators) {
            threads.emplace_back(iterator_thread<KType>, env, env->gen_seed++);
        }
        if (!disable_statistics) {
            threads.emplace_back(statistics_thread<KType>, env);
        }
        if (!disable_clears) {
            threads.emplace_back(clear_thread<KType>, env, env->gen_seed++);
        }
    }
    // Sleeps before ending the threads
    sleep(test_len);
    env->finished.store(true);
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Final size:\t" << env->table.size() << std::endl;
    std::cout << "Final load factor:\t" << env->table.load_factor() << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--thread-num", "--time", "--seed"};
    size_t* arg_vars[] = {&power, &thread_num, &test_len, &seed};
    const char* arg_help[] = {"The power argument given to the hashtable during initialization",
                              "The number of threads to spawn for each type of operation",
                              "The number of seconds to run the test for",
                              "The seed for the random number generator"};
    const char* flags[] = {"--disable-inserts", "--disable-deletes", "--disable-updates",
                           "--disable-finds", "--disable-resizes", "--disable-iterators",
                           "--disable-statistics", "--disable-clears", "--use-strings"};
    bool* flag_vars[] = {&disable_inserts, &disable_deletes, &disable_updates,
                         &disable_finds, &disable_resizes, &disable_iterators,
                         &disable_statistics, &disable_clears, &use_strings};
    const char* flag_help[] = {"If set, no inserts will be run",
                               "If set, no deletes will be run",
                               "If set, no updates will be run",
                               "If set, no finds will be run",
                               "If set, no resize operations will be run",
                               "If set, no iterator operations will be run",
                               "If set, no statistics functions will be run",
                               "If set, no clears will be run",
                               "If set, the key type of the map will be std::string"};
    parse_flags(argc, argv, "Runs a stress test on inserts, deletes, and finds",
                args, arg_vars, arg_help, sizeof(args)/sizeof(const char*), flags,
                flag_vars, flag_help, sizeof(flags)/sizeof(const char*));
    numkeys = (1L << std::max(20uL, power)) * SLOT_PER_BUCKET;

    if (use_strings) {
        auto *env = new AllEnvironment<KeyType2>;
        StressTest(env);
        delete env;
    } else {
        auto *env = new AllEnvironment<KeyType>;
        StressTest(env);
        delete env;
    }
}
