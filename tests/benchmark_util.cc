// Utilities for running benchmarks
#ifndef _BENCHMARK_UTIL_CC
#define _BENCHMARK_UTIL_CC

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>

#if USE_TBB == 1
#include <tbb/concurrent_hash_map.h>
#endif

#if USE_DENSE_HASH == 1
#include <sparsehash/dense_hash_map>
#endif

#include <libcuckoo/cuckoohash_map.hh>
#include <unordered_map>

#include "test_util.cc"

/* Specialization functions for table initialization. The constructor
 * function returns the correct constructor parameters given the
 * expected number of keys. The initialization function does extra
 * initialization for certain table types. */
template <class T>
struct initializer {
    static constexpr size_t construct(const size_t numkeys) {
        return numkeys;
    }
    static void initialize(T& table, const size_t numkeys) {
    }
};

// Since dense_hash_map needs to set an empty key before elements can
// be inserted, we have to do that in the initialize method.
#if USE_DENSE_HASH == 1
template <class KType, class VType>
struct initializer<google::dense_hash_map<KType, VType>> {
    static constexpr size_t construct(const size_t numkeys) {
        return 0;
    }
    static void initialize(google::dense_hash_map<KType, VType>& table, const size_t numkeys) {
        // This shouldn't actually be one of the keys we insert, based
        // on the keys we generate in BenchmarkEnvironment.
        table.set_empty_key(KType());
    }
};
#endif

// Since unordered_map takes a number of buckets as the constructor
// parameter, which is hard to give a good number for, we run reserve
// after construction.
template <class KType, class VType>
struct initializer<std::unordered_map<KType, VType>> {
    static constexpr size_t construct(const size_t numkeys) {
        return 0;
    }
    static void initialize(std::unordered_map<KType, VType>& table, const size_t numkeys) {
        table.reserve(numkeys);
    }
};


/* A specialization that runs inserts for different table types over a
 * range of keys. */
template <class T>
struct inserter {
};

template <class KType, class VType>
struct inserter<cuckoohash_map<KType, VType>> {
    static void fn(cuckoohash_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end) {
        for (;begin != end; begin++) {
            ASSERT_TRUE(table.insert(*begin, 0));
        }
    }
};

#if USE_TBB == 1
template <class KType, class VType>
struct inserter<tbb::concurrent_hash_map<KType, VType>> {
    static void fn(tbb::concurrent_hash_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end) {
        typename tbb::concurrent_hash_map<KType, VType>::accessor a;
        for (;begin != end; begin++) {
            ASSERT_TRUE(table.insert(a, *begin));
            a->second = 0;
        }
    }
};
#endif

#if USE_DENSE_HASH == 1
template <class KType, class VType>
struct inserter<google::dense_hash_map<KType, VType>> {
    static void fn(google::dense_hash_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end) {
        for (;begin != end; begin++) {
            table[*begin] = 0;
        }
    }
};
#endif


template <class KType, class VType>
struct inserter<std::unordered_map<KType, VType>> {
    static void fn(std::unordered_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end) {
        for (;begin != end; begin++) {
            ASSERT_TRUE(table.emplace(*begin, 0).second);
        }
    }
};


/* cacheint is a cache-aligned integer type. */
struct cacheint {
    size_t num;
    cacheint() {
        num = 0;
    }
} __attribute__((aligned(64)));

/* A specialized class that does the reads for different table
 * types. It repeatedly searches for the keys in the given range until
 * the time is up. All the keys in the given range should either be in
 * the table or not in the table. */
template <class T>
struct reader {
};

template <class KType, class VType>
struct reader<cuckoohash_map<KType, VType>> {
    static void fn(cuckoohash_map<KType, VType>& table,
                            typename std::vector<KType>::iterator begin,
                            typename std::vector<KType>::iterator end,
                            cacheint& reads,
                            bool in_table,
                            std::atomic<bool>& finished) {
        VType v;
        while (!finished.load(std::memory_order_acquire)) {
            for (auto it = begin; it != end; it++) {
                if (finished.load(std::memory_order_acquire)) {
                    return;
                }
                ASSERT_EQ(table.find(*it, v), in_table);
                reads.num++;
            }
        }
    }
};

#if USE_TBB == 1
template <class KType, class VType>
struct reader<tbb::concurrent_hash_map<KType, VType>> {
    static void fn(tbb::concurrent_hash_map<KType, VType>& table,
                     typename std::vector<KType>::iterator begin,
                     typename std::vector<KType>::iterator end,
                     cacheint& reads,
                     bool in_table,
                     std::atomic<bool>& finished) {
        typename tbb::concurrent_hash_map<KType, VType>::const_accessor a;
        while (!finished.load(std::memory_order_acquire)) {
            for (auto it = begin; it != end; it++) {
                ASSERT_EQ(table.find(a, *it), in_table);
                reads.num++;
                if (finished.load(std::memory_order_acquire)) {
                    return;
                }
            }
        }
    }
};
#endif

#if USE_DENSE_HASH == 1
template <class KType, class VType>
struct reader<google::dense_hash_map<KType, VType>> {
    static void fn(google::dense_hash_map<KType, VType>& table,
                     typename std::vector<KType>::iterator begin,
                     typename std::vector<KType>::iterator end,
                     cacheint& reads,
                     bool in_table,
                     std::atomic<bool>& finished) {
        auto table_end = table.end();
        while (!finished.load(std::memory_order_acquire)) {
            for (auto it = begin; it != end; it++) {
                ASSERT_EQ((table.find(*it) != table_end), in_table);
                reads.num++;
                if (finished.load(std::memory_order_acquire)) {
                    return;
                }
            }
        }
    }
};
#endif

template <class KType, class VType>
struct reader<std::unordered_map<KType, VType>> {
    static void fn(std::unordered_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end,
                   cacheint& reads,
                   bool in_table,
                   std::atomic<bool>& finished) {
        auto table_end = table.end();
        while (!finished.load(std::memory_order_acquire)) {
            for (auto it = begin; it != end; it++) {
                ASSERT_EQ((table.find(*it) != table_end), in_table);
                reads.num++;
                if (finished.load(std::memory_order_acquire)) {
                    return;
                }
            }
        }
    }
};

/* A specialized class that does a mixture of reads and inserts for
 * different table types. With a certain probability, it inserts the
 * keys in the given range. The rest of the operations are reads on
 * items in the range. It figures out which keys are in the table or
 * not based on where the inserter is. */
template <class T>
struct reader_inserter {
};

template <class KType, class VType>
struct reader_inserter<cuckoohash_map<KType, VType>> {
    static void fn(cuckoohash_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end,
                   const double insert_prob,
                   const size_t start_seed,
                   std::atomic<size_t>& total_ops) {
        VType v;
        std::mt19937_64 gen(start_seed);
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        auto inserter_it = begin;
        auto reader_it = begin;
        size_t ops = 0;
        while (inserter_it != end) {
            if (dist(gen) < insert_prob) {
                // Do an insert
                ASSERT_TRUE(table.insert(*inserter_it, 0));
                inserter_it++;
            } else {
                // Do a read
                ASSERT_EQ(table.find(*reader_it, v), (reader_it < inserter_it));
                reader_it++;
                if (reader_it == end) {
                    reader_it = begin;
                }
            }
            ops++;
        }
        total_ops.fetch_add(ops);
    }
};

#if USE_TBB == 1
template <class KType, class VType>
struct reader_inserter<tbb::concurrent_hash_map<KType, VType>> {
    static void fn(tbb::concurrent_hash_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end,
                   const double insert_prob,
                   const size_t start_seed,
                   std::atomic<size_t>& total_ops) {
        std::mt19937_64 gen(start_seed);
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        auto inserter_it = begin;
        auto reader_it = begin;
        size_t ops = 0;
        typename tbb::concurrent_hash_map<KType, VType>::accessor a;
        typename tbb::concurrent_hash_map<KType, VType>::const_accessor aconst;
        while (inserter_it != end) {
            if (dist(gen) < insert_prob) {
                // Do an insert
                ASSERT_TRUE(table.insert(a, *inserter_it));
                a->second = 0;
                inserter_it++;
                a.release();
            } else {
                // Do a read
                ASSERT_EQ(table.find(aconst, *reader_it), (reader_it < inserter_it));
                reader_it++;
                if (reader_it == end) {
                    reader_it = begin;
                }
                aconst.release();
            }
            ops++;
        }
        total_ops.fetch_add(ops);
    }
};
#endif

#if DENSE_HASH_MAP == 1
template <class KType, class VType>
struct reader_inserter<google::dense_hash_map<KType, VType>> {
    static void fn(google::dense_hash_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end,
                   const double insert_prob,
                   const size_t start_seed,
                   std::atomic<size_t>& total_ops) {
        std::mt19937_64 gen(start_seed);
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        auto inserter_it = begin;
        auto reader_it = begin;
        size_t ops = 0;
        auto table_end = table.end();
        while (inserter_it != end) {
            if (dist(gen) < insert_prob) {
                // Do an insert
                table[*inserter_it] = 0;
                inserter_it++;
            } else {
                // Do a read
                ASSERT_EQ(table.find(*reader_it) != table_end, (reader_it < inserter_it));
                reader_it++;
                if (reader_it == end) {
                    reader_it = begin;
                }
            }
            ops++;
        }
        total_ops.fetch_add(ops);
    }
};
#endif

template <class KType, class VType>
struct reader_inserter<std::unordered_map<KType, VType>> {
    static void fn(std::unordered_map<KType, VType>& table,
                   typename std::vector<KType>::iterator begin,
                   typename std::vector<KType>::iterator end,
                   const double insert_prob,
                   const size_t start_seed,
                   std::atomic<size_t>& total_ops) {
        std::mt19937_64 gen(start_seed);
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        auto inserter_it = begin;
        auto reader_it = begin;
        size_t ops = 0;
        auto table_end = table.end();
        while (inserter_it != end) {
            if (dist(gen) < insert_prob) {
                // Do an insert
                ASSERT_TRUE(table.emplace(*inserter_it, 0).second);
                inserter_it++;
            } else {
                // Do a read
                ASSERT_EQ(table.find(*reader_it) != table_end, (reader_it < inserter_it));
                reader_it++;
                if (reader_it == end) {
                    reader_it = begin;
                }
            }
            ops++;
        }
        total_ops.fetch_add(ops);
    }
};


// BenchmarkEnvironment class, which is the same for all benchmarks
template <class T>
class BenchmarkEnvironment {
    using KType = typename T::key_type;
public:
    BenchmarkEnvironment(const size_t power, const size_t thread_num,
                         const size_t begin_load, size_t& seed) :
        numkeys(1U << power), table(initializer<T>::construct(numkeys)),
        keys(numkeys) {
        // Some table types need extra initialization
        initializer<T>::initialize(table, numkeys);
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
            keys[swapind] = generateKey<KType>(i+numkeys);
        }

        // We prefill the table to begin_load with thread_num threads,
        // giving each thread enough keys to insert
        std::vector<std::thread> threads;
        size_t keys_per_thread = numkeys * (begin_load / 100.0) / thread_num;
        for (size_t i = 0; i < thread_num; i++) {
            threads.emplace_back(inserter<T>::fn, std::ref(table),
                                 keys.begin()+i*keys_per_thread,
                                 keys.begin()+(i+1)*keys_per_thread);
        }
        for (size_t i = 0; i < threads.size(); i++) {
            threads[i].join();
        }

        init_size = table.size();
        ASSERT_TRUE(init_size == keys_per_thread * thread_num);

        std::cout << "Table with capacity " << numkeys <<
            " prefilled to a load factor of " << begin_load << "%" << std::endl;
    }

    size_t numkeys;
    T table;
    std::vector<KType> keys;
    std::mt19937_64 gen;
    size_t init_size;
};

// Table selection logic
typedef enum {
    LIBCUCKOO, TBB, STL, DENSE_HASH
} table_type;

// key and value types used for the tables
using KeyType = uint32_t;
using KeyType2 = std::string;
using ValType = uint32_t;

// check_params checks that the benchmark parameters tt and thread_num
// are set correctly
#define CHECK_PARAMS(tt, thread_num)                                                                                \
    do {                                                                                                            \
        static_assert(!(tt == TBB && !USE_TBB), "Your system doesn't support Intel TBB");                           \
        static_assert(!(tt == DENSE_HASH && !USE_DENSE_HASH), "Your system doesn't support Google dense_hash_map"); \
        if ((tt == STL || tt == DENSE_HASH) && thread_num != 1) {                                                   \
            std::cerr << "Must use 1 thread with STL or DENSE_HASH table types" << std::endl;                       \
            exit(1);                                                                                                \
        }                                                                                                           \
    } while (0)                                                                                                     \

// TABLE_SELECT takes in the table type and use_strings parameters the
// benchmark is using and returns type for the correct table
// type, which can be aliased by the caller
#define KEY_SELECT(use_strings) typename std::conditional<use_strings, KeyType2, KeyType>::type

#if USE_TBB == 1
#define TBB_TABLE(use_strings) typename tbb::concurrent_hash_map<KEY_SELECT(use_strings), ValType>
#else
#define TBB_TABLE(use_strings) void
#endif

#if USE_DENSE_HASH == 1
#define DENSE_HASH_TABLE(use_strings) typename google::dense_hash_map<KEY_SELECT(use_strings), ValType>
#else
#define DENSE_HASH_TABLE(use_strings) void
#endif

#define TABLE_SELECT(tt, use_strings)                                                                                              \
    typename std::conditional<tt == TBB,                                                                                           \
                              TBB_TABLE(use_strings),                                                                              \
                              typename std::conditional<tt == STL,                                                                 \
                                                        typename std::unordered_map<KEY_SELECT(use_strings), ValType>,             \
                                                        typename std::conditional<tt == DENSE_HASH,                                \
                                                                                  DENSE_HASH_TABLE(use_strings),                   \
                                                                                  cuckoohash_map<KEY_SELECT(use_strings), ValType> \
                                                                                  >::type                                          \
                                                        >::type                                                                    \
                              >::type                                                                                              \


#endif
