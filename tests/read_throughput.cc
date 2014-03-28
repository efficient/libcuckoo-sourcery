/* Tests the throughput (queries/sec) of only reads for a specific
 * amount of time in a partially-filled table. */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <stdint.h>
#include <sys/time.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include <libcuckoo/cuckoohash_map.hh>
#include <tbb/concurrent_hash_map.h>
#include "test_util.cc"

typedef uint32_t KeyType;
typedef std::string KeyType2;
typedef uint32_t ValType;

// The number of keys to size the table with, expressed as a power of
// 2. This can be set with the command line flag --power
size_t power = 25;
// The number of threads spawned for inserts. This can be set with the
// command line flag --thread-num
size_t thread_num = sysconf(_SC_NPROCESSORS_ONLN);
// The load factor to fill the table up to before testing throughput.
// This can be set with the --begin-load flag
size_t begin_load = 90;
// The seed which the random number generator uses. This can be set
// with the command line flag --seed
size_t seed = 0;
// How many seconds to run the test for. This can be set with the
// command line flag --time
size_t test_len = 10;
// Whether to use strings as the key
const bool use_strings = false;
// Which table type to use
const table_type tt = LIBCUCKOO;

template <class T>
void ReadThroughputTest(BenchmarkEnvironment<T> *env) {
    std::vector<std::thread> threads;
    std::vector<cacheint> counters(thread_num);
    // We use the first half of the threads to read the init_size
    // elements that are in the table and the other half to read the
    // numkeys-init_size elements that aren't in the table.
    const size_t first_threadnum = thread_num / 2;
    const size_t second_threadnum = thread_num - thread_num / 2;
    const size_t in_keys_per_thread = (first_threadnum == 0) ? 0 : env->init_size / first_threadnum;
    const size_t out_keys_per_thread = (env->numkeys - env->init_size) / second_threadnum;
    // When set to true, it signals to the threads to stop running
    std::atomic<bool> finished(false);
    for (size_t i = 0; i < first_threadnum; i++) {
        threads.emplace_back(reader<T>::fn, std::ref(env->table),
                             env->keys.begin() + (i*in_keys_per_thread),
                             env->keys.begin() + ((i+1)*in_keys_per_thread),
                             std::ref(counters[i]), true, std::ref(finished));
    }
    for (size_t i = 0; i < second_threadnum; i++) {
        threads.emplace_back(reader<T>::fn, std::ref(env->table),
                             env->keys.begin() + (i*out_keys_per_thread) + env->init_size,
                             env->keys.begin() + (i+1)*out_keys_per_thread + env->init_size,
                             std::ref(counters[first_threadnum+i]), false, std::ref(finished));
    }
    sleep(test_len);
    finished.store(true, std::memory_order_release);
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    size_t total_reads = 0;
    for (size_t i = 0; i < counters.size(); i++) {
        total_reads += counters[i].num;
    }
    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Number of reads:\t" << total_reads << std::endl;
    std::cout << "Time elapsed:\t" << test_len << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << total_reads / (double)test_len << " reads/sec" << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--thread-num", "--begin-load", "--time", "--seed"};
    size_t* arg_vars[] = {&power, &thread_num, &begin_load, &test_len, &seed};
    const char* arg_help[] = {"The number of keys to size the table with, expressed as a power of 2",
                              "The number of threads to spawn for each type of operation",
                              "The load factor to fill the table up to before testing reads",
                              "The number of seconds to run the test for",
                              "The seed used by the random number generator"};
    const char* flags[] = {};
    bool* flag_vars[] = {};
    const char* flag_help[] = {};
    parse_flags(argc, argv, "A benchmark for inserts",
                args, arg_vars, arg_help, sizeof(args)/sizeof(const char*),
                flags, flag_vars, flag_help, sizeof(flags)/sizeof(const char*));

    using Table = TABLE_SELECT(tt);
    auto *env = new BenchmarkEnvironment<Table>(power, thread_num, begin_load,seed);
    ReadThroughputTest(env);
    delete env;
}
