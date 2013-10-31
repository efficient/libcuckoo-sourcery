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
#include <cassert>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>

#include "commandline_parser.cc"
extern "C" {
#include "cuckoohash.h"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
}

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

struct insert_thread_args {
    std::vector<KeyType>::iterator begin;
    std::vector<KeyType>::iterator end;
    cuckoo_hashtable_t* table;
};

// Inserts the keys in the given range in a random order, avoiding
// inserting duplicates
static void *insert_thread(void* arg) {
    insert_thread_args *it_args = (insert_thread_args*)arg;
    auto begin = it_args->begin;
    auto end = it_args->end;
    auto table = it_args->table;
    ValType val = 0;
    for (;begin != end; begin++) {
        if (table->hashpower > power) {
            std::cerr << "Expansion triggered" << std::endl;
            exit(1);
        }
        KeyType key = *begin;
        cuckoo_status res = cuckoo_insert(table, (const char*) &key, (const char*) &val);
        if (res != ok) {
            std::cerr << "Failed insert with code " << res << std::endl;
            exit(1);
        }
    }
}

class InsertEnvironment {
public:
    // We allocate the vectors with the total amount of space in the
    // table, which is bucket_count() * SLOT_PER_BUCKET
    InsertEnvironment()
        : table(cuckoo_init(power)), numkeys((1 << table->hashpower)*bucketsize), keys(numkeys) {}

    void SetUp() {
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
        pthread_t* threads = new pthread_t[thread_num];
        insert_thread_args *threadargs = new insert_thread_args[thread_num];
        size_t keys_per_thread = numkeys * (begin_load / 100.0) / thread_num;
        for (size_t i = 0; i < thread_num; i++) {
            threadargs[i] = {keys.begin()+i*keys_per_thread, keys.begin()+(i+1)*keys_per_thread, table};
            pthread_create(&threads[i], NULL, insert_thread, &threadargs[i]);
        }
        for (size_t i = 0; i < thread_num; i++) {
            pthread_join(threads[i], NULL);
        }
        delete threads;
        delete threadargs;
        init_size = keys_per_thread * thread_num;

        std::cout << "Table with capacity " << numkeys << " prefilled to a load factor of " << cuckoo_loadfactor(table) << std::endl;
    }

    void TearDown() {
        cuckoo_exit(table);
    }

    cuckoo_hashtable_t* table;
    size_t numkeys;
    std::vector<KeyType> keys;
    std::mt19937_64 gen;
    size_t init_size;
};

InsertEnvironment* env;

void TestEverything() {
    pthread_t* threads = new pthread_t[thread_num];
    insert_thread_args *threadargs = new insert_thread_args[thread_num];
    size_t keys_per_thread = env->numkeys * ((end_load - begin_load) / 100.0) / thread_num;
    timeval t1, t2;
    gettimeofday(&t1, NULL);
    for (size_t i = 0; i < thread_num; i++) {
        threadargs[i] = {env->keys.begin()+(i*keys_per_thread)+env->init_size, env->keys.begin()+((i+1)*keys_per_thread)+env->init_size, env->table};
        pthread_create(&threads[i], NULL, insert_thread, &threadargs[i]);
    }
    for (size_t i = 0; i < thread_num; i++) {
        pthread_join(threads[i], NULL);
    }
    delete threads;
    delete threadargs;
    gettimeofday(&t2, NULL);
    double elapsed_time = (t2.tv_sec - t1.tv_sec) * 1000.0; // sec to ms
    elapsed_time += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    size_t num_inserts = keys_per_thread * thread_num;
    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Final load factor:\t" << cuckoo_loadfactor(env->table) << std::endl;
    std::cout << "Number of inserts:\t" << num_inserts << std::endl;
    std::cout << "Time elapsed:\t" << elapsed_time << " milliseconds" << std::endl;
    std::cout << "Throughput: " << (double)num_inserts / elapsed_time << " inserts/ms" << std::endl;
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

    env = new InsertEnvironment;
    env->SetUp();
    TestEverything();
    env->TearDown();
}
