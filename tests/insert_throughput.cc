/* Tests the throughput (queries/sec) of only inserts between a
 * specific load range in a partially-filled table */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <functional>
#include <iostream>
#include <thread>
#include <vector>

#include "benchmark_util.cc"

// Whether to use strings as the key
const bool use_strings = false;
// Which table type to use
const table_type tt = LIBCUCKOO;

template <class T>
void InsertThroughputTest(BenchmarkEnvironment<T> *env) {
    std::vector<std::thread> threads;
    size_t keys_per_thread = env->numkeys * ((end_load-begin_load) / 100.0) / thread_num;
    timeval t1, t2;
    gettimeofday(&t1, NULL);
    for (size_t i = 0; i < thread_num; i++) {
        threads.emplace_back(inserter<T>::fn, std::ref(env->table),
                             env->keys.begin()+(i*keys_per_thread)+env->init_size,
                             env->keys.begin()+((i+1)*keys_per_thread)+env->init_size);
    }
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    gettimeofday(&t2, NULL);
    double elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000.0; // sec to ms
    elapsed_time += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    size_t num_inserts = env->table.size() - env->init_size;
    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Final load factor:\t" << end_load << "%" << std::endl;
    std::cout << "Number of inserts:\t" << num_inserts << std::endl;
    std::cout << "Time elapsed:\t" << elapsed_time/1000 << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << (double)num_inserts / (elapsed_time/1000) << " inserts/sec" << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--thread-num", "--begin-load", "--end-load", "--seed"};
    size_t* arg_vars[] = {&power, &thread_num, &begin_load, &end_load, &seed};
    const char* arg_help[] = {"The number of keys to size the table with, expressed as a power of 2",
                              "The number of threads to spawn for each type of operation",
                              "The load factor to fill the table up to before testing throughput",
                              "The maximum load factor to fill the table up to when testing throughput",
                              "The seed used by the random number generator"};
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
    }

    CHECK_PARAMS(tt, thread_num);
    using Table = TABLE_SELECT(tt, use_strings);
    auto *env = new BenchmarkEnvironment<Table>(power, thread_num, begin_load,seed);
    InsertThroughputTest(env);
    delete env;
}
