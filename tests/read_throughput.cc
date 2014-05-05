/* Tests the throughput (queries/sec) of only reads for a specific
 * amount of time in a partially-filled table. */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <functional>
#include <iostream>
#include <thread>
#include <vector>
#include <sys/time.h>

#include "benchmark_util.cc"

// The number of keys to size the table with, expressed as a power of
// 2. This can be set with the command line flag --power
size_t power = 25;
// The number of threads spawned for inserts. This can be set with the
// command line flag --thread-num
const size_t thread_num = 8;
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
void ReadThroughputTest(BenchmarkEnvironment<T, thread_num> *env) {
    std::vector<std::thread> threads;
    std::atomic<size_t> total_reads(0);
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
        threads.emplace_back(reader<T, thread_num>::fn, std::ref(env->table),
                             env->keys.begin() + (i*in_keys_per_thread),
                             env->keys.begin() + ((i+1)*in_keys_per_thread),
                             std::ref(total_reads), true, std::ref(finished));
    }
    for (size_t i = 0; i < second_threadnum; i++) {
        threads.emplace_back(reader<T, thread_num>::fn, std::ref(env->table),
                             env->keys.begin() + (i*out_keys_per_thread) + env->init_size,
                             env->keys.begin() + (i+1)*out_keys_per_thread + env->init_size,
                             std::ref(total_reads), false, std::ref(finished));
    }
    sleep(test_len);
    finished.store(true, std::memory_order_release);
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }

    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Number of reads:\t" << total_reads.load() << std::endl;
    std::cout << "Time elapsed:\t" << test_len << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << total_reads.load() / (double)test_len << " reads/sec" << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--begin-load", "--time", "--seed"};
    size_t* arg_vars[] = {&power, &begin_load, &test_len, &seed};
    const char* arg_help[] = {"The number of keys to size the table with, expressed as a power of 2",
                              "The load factor to fill the table up to before testing reads",
                              "The number of seconds to run the test for",
                              "The seed used by the random number generator"};
    const char* flags[] = {};
    bool* flag_vars[] = {};
    const char* flag_help[] = {};
    parse_flags(argc, argv, "A benchmark for inserts",
                args, arg_vars, arg_help, sizeof(args)/sizeof(const char*),
                flags, flag_vars, flag_help, sizeof(flags)/sizeof(const char*));

    CHECK_PARAMS(tt, thread_num);
    using Table = TABLE_SELECT(tt, use_strings);
    auto *env = new BenchmarkEnvironment<Table, thread_num>(power, begin_load,seed);
    ReadThroughputTest(env);
    delete env;
}
