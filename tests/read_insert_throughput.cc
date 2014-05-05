/* Tests the throughput (queries/sec) of only inserts between a
 * specific load range in a partially-filled table */

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
// This can be set with the command line flag --begin-load.
size_t begin_load = 0;
// The maximum load factor to fill the table up to when testing
// throughput. This can be set with the command line flag
// --end-load.
size_t end_load = 90;
// The seed which the random number generator uses. This can be set
// with the command line flag --seed
size_t seed = 0;
// The percentage of operations that should be inserts. This should be
// at least 10. This can be set with the command line flag
// --insert-percent
size_t insert_percent = 10;
// Whether to use strings as the key
const bool use_strings = false;
// Which table type to use
const table_type tt = LIBCUCKOO;

template <class T>
void ReadInsertThroughputTest(BenchmarkEnvironment<T, thread_num> *env) {
    const size_t start_seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::atomic<size_t> total_ops(0);
    std::vector<std::thread> threads;
    size_t keys_per_thread = env->numkeys * ((end_load-begin_load) / 100.0) / thread_num;
    timeval t1, t2;
    gettimeofday(&t1, NULL);
    for (size_t i = 0; i < thread_num; i++) {
        threads.emplace_back(reader_inserter<T, thread_num>::fn, std::ref(env->table),
                             env->keys.begin()+(i*keys_per_thread)+env->init_size,
                             env->keys.begin()+((i+1)*keys_per_thread)+env->init_size,
                             (double)insert_percent / 100.0, start_seed + i, std::ref(total_ops));
    }
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    gettimeofday(&t2, NULL);
    double elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000.0; // sec to ms
    elapsed_time += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Final load factor:\t" << end_load << "%" << std::endl;
    std::cout << "Number of operations:\t" << total_ops.load() << std::endl;
    std::cout << "Time elapsed:\t" << elapsed_time/1000 << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << (double)total_ops.load() / (elapsed_time/1000) << " ops/sec" << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--begin-load", "--end-load", "--seed", "--insert-percent"};
    size_t* arg_vars[] = {&power, &begin_load, &end_load, &seed, &insert_percent};
    const char* arg_help[] = {"The number of keys to size the table with, expressed as a power of 2",
                              "The load factor to fill the table up to before testing throughput",
                              "The maximum load factor to fill the table up to when testing throughput",
                              "The seed used by the random number generator",
                              "The percentage of operations that should be inserts"};
    const char* flags[] = {};
    bool* flag_vars[] = {};
    const char* flag_help[] = {};
    parse_flags(argc, argv, "A benchmark for inserts",
                args, arg_vars, arg_help, sizeof(args)/sizeof(const char*),
                flags, flag_vars, flag_help, sizeof(flags)/sizeof(const char*));

    if (begin_load >= 100) {
        std::cerr << "--begin-load must be between 0 and 99" << std::endl;
        exit(1);
    } else if (begin_load >= end_load) {
        std::cerr << "--end-load must be greater than --begin-load" << std::endl;
        exit(1);
    } else if (insert_percent < 10 || insert_percent >= 100) {
        std::cerr << "--insert-percent must be between 10 and 99" << std::endl;
        exit(1);
    }

    CHECK_PARAMS(tt, thread_num);
    using Table = TABLE_SELECT(tt, use_strings);
    auto *env = new BenchmarkEnvironment<Table, thread_num>(power, begin_load,seed);
    ReadInsertThroughputTest(env);
    delete env;
}
