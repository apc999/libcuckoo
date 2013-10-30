/* Tests the throughput (queries/sec) of only inserts between a
 * specific load range in a partially-filled table */
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
#include <unistd.h>

#include "commandline_parser.cc"
#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "gtest/gtest.h"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef std::pair<KeyType, ValType> KVPair;

// The power argument passed to the hashtable constructor. This can be
// set with the command line flag --power.
size_t power = 19;
// The number of threads spawned for inserts. This can be set with the
// command line flag --thread-num
size_t thread_num = sysconf(_SC_NPROCESSORS_ONLN);
// The load factor to fill the table up to before testing throughput.
// This can be set with the command line flag --begin-load.
size_t begin_load = 50;
// The maximum load factor to fill the table up to when testing
// throughput. This can be set with the command line flag
// --end-load.
size_t end_load = 75;
// The seed which the random number generator uses. This can be set
// with the command line flag --seed
size_t seed = 0;

// Inserts the keys in the given range (with value 0), exiting if there is an expansion
void insert_thread(cuckoohash_map<KeyType, ValType>& table, std::vector<KeyType>::iterator begin, std::vector<KeyType>::iterator end) {
    for (;begin != end; begin++) {
        if (table.hashpower() > power) {
            std::cerr << "Expansion triggered" << std::endl;
            exit(1);
        }
        ASSERT_TRUE(table.insert(*begin, 0));
    }
}

class InsertEnvironment : public ::testing::Environment {
public:
    // We allocate the vectors with the total amount of space in the
    // table, which is bucket_count() * SLOT_PER_BUCKET
    InsertEnvironment()
        : table(power), numkeys(table.bucket_count()*SLOT_PER_BUCKET), keys(numkeys) {}

    virtual void SetUp() {
        // Sets up the random number generator
        if (seed == 0) {
            seed = std::chrono::system_clock::now().time_since_epoch().count();
        }
        std::cout << "seed = " << seed << std::endl;
        gen.seed(seed);

        // We fill the keys array with integers between numkeys and
        // 2*numkeys, shuffled randomly
        keys[0] = numkeys;
        for (size_t i = 1; i < numkeys; i++) {
            const size_t swapind = gen() % i;
            keys[i] = keys[swapind];
            keys[swapind] = i+numkeys;
        }

        // We prefill the table to begin_load with as many threads as
        // there are processors, giving each thread enough keys to
        // insert
        std::vector<std::thread> threads;
        const size_t max_thread_num = sysconf(_SC_NPROCESSORS_ONLN);
        size_t keys_per_thread = numkeys * (begin_load / 100.0) / max_thread_num;
        for (size_t i = 0; i < max_thread_num; i++) {
            threads.emplace_back(insert_thread, std::ref(table), keys.begin()+i*keys_per_thread, keys.begin()+(i+1)*keys_per_thread);
        }
        for (size_t i = 0; i < threads.size(); i++) {
            threads[i].join();
        }

        init_size = table.size();

        std::cout << "Table with capacity " << numkeys << " prefilled to a load factor of " << table.load_factor() << std::endl;
    }

    cuckoohash_map<KeyType, ValType> table;
    size_t numkeys;
    std::vector<KeyType> keys;
    std::mt19937_64 gen;
    size_t init_size;
};

InsertEnvironment* env;

TEST(InsertThroughputTest, Everything) {
    std::vector<std::thread> threads;
    size_t keys_per_thread = env->numkeys * ((end_load-begin_load) / 100.0) / thread_num;
    auto t1 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < thread_num; i++) {
        threads.emplace_back(insert_thread, std::ref(env->table), env->keys.begin()+(i*keys_per_thread)+env->init_size, env->keys.begin()+((i+1)*keys_per_thread)+env->init_size);
    }
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    auto t2 = std::chrono::high_resolution_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1);
    size_t num_inserts = env->table.size() - env->init_size;
    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Final load factor:\t" << env->table.load_factor() << std::endl;
    std::cout << "Number of inserts:\t" << num_inserts << std::endl;
    std::cout << "Time elapsed:\t" << elapsed_time.count() << " milliseconds" << std::endl;
    std::cout << "Throughput: " << (double)num_inserts / (double)elapsed_time.count() << " inserts/ms" << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--thread-num", "--begin-load", "--end-load", "--seed"};
    size_t* arg_vars[] = {&power, &thread_num, &begin_load, &end_load, &seed};
    const char* arg_help[] = {"The power argument given to the hashtable during initialization",
                              "The number of threads to spawn for each type of operation",
                              "The load factor to fill the table up to before testing throughput",
                              "The maximum load factor to fill the table up to when testing throughput",
                              "The seed used by the random number generator"};
    parse_flags(argc, argv, args, arg_vars, arg_help, sizeof(args)/sizeof(const char*), nullptr, nullptr, nullptr, 0);

    if (begin_load >= 100) {
        std::cerr << "--begin-load must be between 0 and 99" << std::endl;
        exit(1);
    } else if (begin_load >= end_load) {
        std::cerr << "--end-load must be greater than --begin-load" << std::endl;
        exit(1);
    }

    env = (InsertEnvironment*) ::testing::AddGlobalTestEnvironment(new InsertEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
