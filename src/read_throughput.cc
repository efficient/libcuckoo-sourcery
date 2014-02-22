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
#include <cassert>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <numeric>

#include "commandline_parser.cc"
extern "C" {
#include "cuckoohash.h"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET
}

// The power argument passed to the hashtable constructor. This can be
// set with the command line flag --power.
size_t power = 23;
// The number of threads spawned for inserts. This can be set with the
// command line flag --thread-num
size_t thread_num = sysconf(_SC_NPROCESSORS_ONLN);
// The load factor to fill the table up to before testing throughput.
// This can be set with the --load flag
size_t load = 50;
// The seed which the random number generator uses. This can be set
// with the command line flag --seed
size_t seed = 0;
// How many seconds to run the test for. This can be set with the
// command line flag --time
size_t test_len = 10;

// When set to true, it signals to the threads to stop running
std::atomic<bool> finished = ATOMIC_VAR_INIT(false);

/* cacheint is a cache-aligned integer type. */
struct cacheint {
    size_t num;
    cacheint() {
        num = 0;
    }
} __attribute__((aligned(64)));

struct thread_args {
    std::vector<KeyType>::iterator begin;
    std::vector<KeyType>::iterator end;
    cuckoo_hashtable_t* table;
    cacheint* reads;
    bool in_table;
};

// Repeatedly searches for the keys in the given range until the time
// is up. All the keys in the given range should either be in the
// table or not in the table.
static void *read_thread(void* arg) {
    thread_args *rt_args = (thread_args*)arg;
    auto begin = rt_args->begin;
    auto end = rt_args->end;
    auto table = rt_args->table;
    cacheint *reads = rt_args->reads;
    bool in_table = rt_args->in_table;
    ValType v;
    while (true) {
        for (auto it = begin; it != end; it++) {
            if (finished.load(std::memory_order_acquire)) {
                return arg;
            }
            KeyType key = *it;
            bool found = cuckoo_find(table, (const char*)&key, (char*)&v) == ok;
            if (found != in_table) {
                std::cerr << found << " doesn't match expected result of " << in_table << std::endl;
                exit(1);
            }
            reads->num++;
        }
    }
}

// Inserts the keys in the given range in a random order, avoiding
// inserting duplicates
static void *insert_thread(void* arg) {
    thread_args *it_args = (thread_args*)arg;
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

class ReadEnvironment {
public:
    // We allocate the vectors with the total amount of space in the
    // table, which is bucket_count() * SLOT_PER_BUCKET
    ReadEnvironment()
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

        // We prefill the table to load with as many threads as
        // there are processors, giving each thread enough keys to
        // insert
        pthread_t *threads = new pthread_t[thread_num];
        thread_args *threadargs = new thread_args[thread_num];
        size_t keys_per_thread = numkeys * (load / 100.0) / thread_num;
        for (size_t i = 0; i < thread_num; i++) {
            threadargs[i] = {keys.begin()+i*keys_per_thread, keys.begin()+(i+1)*keys_per_thread, table, nullptr, false};
            pthread_create(&threads[i], NULL, insert_thread, &threadargs[i]);
        }
        for (size_t i = 0; i < thread_num; i++) {
            pthread_join(threads[i], NULL);
        }
        delete threads;
        delete threadargs;
        init_size = keys_per_thread * thread_num;

        std::cout << "Table with capacity " << numkeys << " prefilled to a load factor of " << load << std::endl;
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

ReadEnvironment* env;

void TestEverything() {
    pthread_t* threads = new pthread_t[thread_num];
    thread_args *threadargs = new thread_args[thread_num];
    cacheint *counters = new cacheint[thread_num];
    // We use the first half of the threads to read the init_size
    // elements that are in the table and the other half to read the
    // numkeys-init_size elements that aren't in the table.
    const size_t first_threadnum = thread_num / 2;
    const size_t second_threadnum = thread_num - first_threadnum;
    size_t in_keys_per_thread = (first_threadnum == 0) ? 0 : env->init_size / (thread_num / 2);
    size_t out_keys_per_thread = (env->numkeys - env->init_size) / second_threadnum;
    ValType v;
    for (size_t i = 0; i < first_threadnum; i++) {
        threadargs[i] = {env->keys.begin()+(i*in_keys_per_thread), env->keys.begin()+((i+1)*in_keys_per_thread), env->table, &counters[i], true};
        pthread_create(&threads[i], NULL, read_thread, &threadargs[i]);
    }
    for (size_t i = 0; i < second_threadnum; i++) {
        threadargs[first_threadnum+i] = {env->keys.begin()+(i*out_keys_per_thread)+env->init_size, env->keys.begin()+((i+1)*out_keys_per_thread)+env->init_size, env->table, &counters[first_threadnum+i], false};
        pthread_create(&threads[first_threadnum+i], NULL, read_thread, &threadargs[first_threadnum+i]);
    }
    sleep(test_len);
    finished.store(true, std::memory_order_relaxed);
    for (size_t i = 0; i < thread_num; i++) {
        pthread_join(threads[i], NULL);
    }
    delete threads;
    delete threadargs;
    size_t total_reads = 0;
    for (size_t i = 0; i < thread_num; i++) {
        total_reads += counters[i].num;
    }
    delete counters;
    // Reports the results
    std::cout << "----------Results----------" << std::endl;
    std::cout << "Number of reads:\t" << total_reads << std::endl;
    std::cout << "Time elapsed:\t" << test_len << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << total_reads / (double)test_len << " inserts/sec" << std::endl;
}

int main(int argc, char** argv) {
    const char* args[] = {"--power", "--thread-num", "--load", "--time", "--seed"};
    size_t* arg_vars[] = {&power, &thread_num, &load, &test_len, &seed};
    const char* arg_help[] = {"The power argument given to the hashtable during initialization",
                              "The number of threads to spawn for each type of operation",
                              "The load factor to fill the table up to before testing reads",
                              "The number of seconds to run the test for"
                              "The seed used by the random number generator"};
    parse_flags(argc, argv, args, arg_vars, arg_help, sizeof(args)/sizeof(const char*), nullptr, nullptr, nullptr, 0);

    env = new ReadEnvironment;
    env->SetUp();
    TestEverything();
    env->TearDown();
}
