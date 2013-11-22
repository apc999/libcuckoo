/* Tests the throughput (queries/sec) of only reads for a specific
 * amount of time in a partially-filled table. */

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
#include <sys/time.h>
#include <numeric>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
#include "test_util.cc"

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef std::pair<KeyType, ValType> KVPair;

// The power argument passed to the hashtable constructor. This can be
// set with the command line flag --power.
size_t power = 23;
// The number of threads spawned for inserts. This can be set with the
// command line flag --thread-num
size_t thread_num = sysconf(_SC_NPROCESSORS_ONLN);
// The load factor to fill the table up to before testing throughput.
// This can't be set
size_t partial_load = 50;
// The seed which the random number generator uses. This can be set
// with the command line flag --seed
size_t seed = 0;
// How many seconds to run the test for. This can be set with the
// command line flag --time
size_t test_len = 10;

// When set to true, it signals to the threads to stop running
std::atomic<bool> finished = ATOMIC_VAR_INIT(false);

// Repeatedly searches for the keys in the given range until the time
// is up. All the keys in the given range should either be in the
// table or not in the table.
void read_thread(cuckoohash_map<KeyType, ValType>& table, std::vector<KeyType>::iterator begin,
                 std::vector<KeyType>::iterator end, const bool in_table, size_t& reads) {
    ValType v;
    while (true) {
        for (auto it = begin; it != end; it++) {
            if (finished.load(std::memory_order_acquire)) {
                return;
            }
            ASSERT_EQ(table.find(*begin, v), in_table);
            reads++;
        }
    }
}

// Inserts the keys in the given range (with value 0), exiting if
// there is an expansion
void insert_thread(cuckoohash_map<KeyType, ValType>& table, std::vector<KeyType>::iterator begin, std::vector<KeyType>::iterator end) {
    for (;begin != end; begin++) {
        if (table.hashpower() > power) {
            std::cerr << "Expansion triggered" << std::endl;
            exit(1);
        }
        ASSERT_TRUE(table.insert(*begin, 0));
    }
}


class ReadEnvironment {
public:
    // We allocate the vectors with the total amount of space in the
    // table, which is bucket_count() * SLOT_PER_BUCKET
    ReadEnvironment()
        : table(power), numkeys(table.bucket_count()*SLOT_PER_BUCKET), keys(numkeys) {
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

        // We prefill the table to partial_load with thread_num
        // threads, giving each thread enough keys to insert
        std::vector<std::thread> threads;
        size_t keys_per_thread = numkeys * (partial_load / 100.0) / thread_num;
        for (size_t i = 0; i < thread_num; i++) {
            threads.emplace_back(insert_thread, std::ref(table), keys.begin()+i*keys_per_thread, keys.begin()+(i+1)*keys_per_thread);
        }
        for (size_t i = 0; i < threads.size(); i++) {
            threads[i].join();
        }

        init_size = table.size();
        ASSERT_TRUE(init_size == keys_per_thread * thread_num);

        std::cout << "Table with capacity " << numkeys << " prefilled to a load factor of " << table.load_factor() << std::endl;
    }

    cuckoohash_map<KeyType, ValType> table;
    size_t numkeys;
    std::vector<KeyType> keys;
    std::mt19937_64 gen;
    size_t init_size;
};


ReadEnvironment* env;

void ReadThroughputTest() {
    std::vector<std::thread> threads;
    std::vector<size_t> counters(thread_num);
    // We use the first half of the threads to read the init_size
    // elements that are in the table and the other half to read the
    // numkeys-init_size elements that aren't in the table.
    const size_t first_threadnum = thread_num / 2;
    const size_t second_threadnum = thread_num - thread_num / 2;
    const size_t in_keys_per_thread = (first_threadnum == 0) ? 0 : env->init_size / first_threadnum;
    const size_t out_keys_per_thread = (env->numkeys - env->init_size) / second_threadnum;
    for (size_t i = 0; i < first_threadnum; i++) {
        threads.emplace_back(read_thread, std::ref(env->table), env->keys.begin() + (i*in_keys_per_thread),
                             env->keys.begin() + ((i+1)*in_keys_per_thread), true, std::ref(counters[i]));
    }
    for (size_t i = 0; i < second_threadnum; i++) {
        threads.emplace_back(read_thread, std::ref(env->table), env->keys.begin() + (i*out_keys_per_thread) + env->init_size,
                             env->keys.begin() + ((i+1)*out_keys_per_thread + env->init_size), false,
                             std::ref(counters[first_threadnum+i]));
    }
    sleep(test_len);
    finished.store(true, std::memory_order_release);
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    size_t total_reads = std::accumulate(counters.begin(), counters.end(), 0);
    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Number of reads:\t" << total_reads << std::endl;
    std::cout << "Time elapsed:\t" << test_len << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << total_reads / (double)test_len << " reads/sec" << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--thread-num", "--time", "--seed"};
    size_t* arg_vars[] = {&power, &thread_num, &test_len, &seed};
    const char* arg_help[] = {"The power argument given to the hashtable during initialization",
                              "The number of threads to spawn for each type of operation",
                              "The number of seconds to run the test for"
                              "The seed used by the random number generator"};
    parse_flags(argc, argv, args, arg_vars, arg_help, sizeof(args)/sizeof(const char*), nullptr, nullptr, nullptr, 0);

    env = new ReadEnvironment;
    ReadThroughputTest();
}
