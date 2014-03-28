#ifndef _TEST_UTIL_CC
#define _TEST_UTIL_CC

// Utilities for running tests
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <libcuckoo/cuckoohash_map.hh>
#include <tbb/concurrent_hash_map.h>
#include <unordered_map>

std::mutex print_lock;
typedef std::lock_guard<std::mutex> mutex_guard;

// Prints a message if the two items aren't equal
template <class T>
inline void do_expect_equal(T x, const char *xname, T y, const char *yname, size_t line) {
    if (x != y) {
        mutex_guard m(print_lock);
        std::cout << "ERROR:\t" << xname << "(" << x << ") does not equal " << yname << "(" << y << ") on line " << line << std::endl;
    }
}
#define EXPECT_EQ(x, y) do_expect_equal(x, #x, y, #y, __LINE__)

// Prints a message if the two items are equal
template <class T>
inline void do_expect_not_equal(T x, const char *xname, T y, const char *yname, size_t line) {
    if (x == y) {
        mutex_guard m(print_lock);
        std::cout << "ERROR:\t" << xname << "(" << x << ") equals " << yname << "(" << y << ") on line " << line << std::endl;
    }
}
#define EXPECT_NE(x, y) do_expect_not_equal(x, #x, y, #y, __LINE__)

// Prints a message if the item is false
inline void do_expect_true(bool x, const char *xname, size_t line) {
    if (!x) {
        mutex_guard m(print_lock);
        std::cout << "ERROR:\t" << xname << "(" << x << ") is false on line " << line << std::endl;
    }
}
#define EXPECT_TRUE(x) do_expect_true(x, #x, __LINE__)

// Prints a message if the item is true
inline void do_expect_false(bool x, const char *xname, size_t line) {
    if (x) {
        mutex_guard m(print_lock);
        std::cout << "ERROR:\t" << xname << "(" << x << ") is true on line " << line << std::endl;
    }
}
#define EXPECT_FALSE(x) do_expect_false(x, #x, __LINE__)

// Prints a message and exists if the two items aren't equal
template <class T>
inline void do_assert_equal(T x, const char *xname, T y, const char *yname, size_t line) {
    if (x != y) {
        mutex_guard m(print_lock);
        std::cout << "FATAL ERROR:\t" << xname << "(" << x << ") does not equal " << yname << "(" << y << ") on line " << line << std::endl;
        exit(1);
    }
}
#define ASSERT_EQ(x, y) do_assert_equal(x, #x, y, #y, __LINE__)

// Prints a message and exists if the item is false
inline void do_assert_true(bool x, const char *xname, size_t line) {
    if (!x) {
        mutex_guard m(print_lock);
        std::cout << "FATAL ERROR:\t" << xname << "(" << x << ") is false on line " << line << std::endl;
        exit(1);
    }
}
#define ASSERT_TRUE(x) do_assert_true(x, #x, __LINE__)


// Parses boolean flags and flags with positive integer arguments
void parse_flags(int argc, char**argv, const char* description,
                 const char* args[], size_t* arg_vars[], const char* arg_help[], size_t arg_num,
                 const char* flags[], bool* flag_vars[], const char* flag_help[], size_t flag_num) {

    errno = 0;
    for (int i = 0; i < argc; i++) {
        for (size_t j = 0; j < arg_num; j++) {
            if (strcmp(argv[i], args[j]) == 0) {
                if (i == argc-1) {
                    std::cerr << "You must provide a positive integer argument after the " << args[j] << " argument" << std::endl;
                    exit(1);
                } else {
                    size_t argval = strtoull(argv[i+1], NULL, 10);
                    if (errno != 0) {
                        std::cerr << "The argument to " << args[j] << " must be a valid size_t" << std::endl;
                        exit(1);
                    } else {
                        *(arg_vars[j]) = argval;
                    }
                }
            }
        }
        for (size_t j = 0; j < flag_num; j++) {
            if (strcmp(argv[i], flags[j]) == 0) {
                *(flag_vars[j]) = true;
            }
        }
        if (strcmp(argv[i], "--help") == 0) {
            std::cerr << description << std::endl;
            std::cerr << "Arguments:" << std::endl;
            for (size_t j = 0; j < arg_num; j++) {
                std::cerr << args[j] << " (default " << *arg_vars[j] << "):\t" << arg_help[j] << std::endl;
            }
            for (size_t j = 0; j < flag_num; j++) {
                std::cerr << flags[j] << " (default " << (*flag_vars[j] ? "true" : "false") << "):\t" << flag_help[j] << std::endl;
            }
            exit(0);
        }
    }
}

/* generateKey is a function from a number to another given type, used
 * to generate keys for insertion. */
template <class T>
T generateKey(size_t i) {
    return (T)i;
}
/* This specialization returns a stringified representation of the
 * given integer, where the number is copied to the end of a long
 * string of 'a's, in order to make comparisons and hashing take
 * time. */
template <>
std::string generateKey<std::string>(size_t i) {
    const size_t min_length = 100;
    const std::string num(std::to_string(i));
    if (num.size() >= min_length) {
        return num;
    }
    std::string ret(min_length, 'a');
    const size_t startret = min_length - num.size();
    for (size_t i = 0; i < num.size(); i++) {
        ret[i+startret] = num[i];
    }
    return ret;
}

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
    LIBCUCKOO, TBB, STL
} table_type;

#define TABLE_SELECT(tt)                                                                                                          \
    typename std::conditional<tt == TBB,                                                                                          \
                              typename std::conditional<use_strings,                                                              \
                                                        typename tbb::concurrent_hash_map<KeyType2, ValType>,                     \
                                                        typename tbb::concurrent_hash_map<KeyType, ValType>                       \
                                                        >::type,                                                                  \
                              typename std::conditional<tt == LIBCUCKOO,                                                          \
                                                        typename std::conditional<use_strings,                                    \
                                                                                  cuckoohash_map<KeyType2, ValType>,              \
                                                                                  cuckoohash_map<KeyType, ValType>                \
                                                                                  >::type,                                        \
                                                        typename std::conditional<use_strings,                                    \
                                                                                  typename std::unordered_map<KeyType2, ValType>, \
                                                                                  typename std::unordered_map<KeyType, ValType>   \
                                                                                  >::type                                         \
                                                        >::type                                                                   \
                              >::type; do { if(tt == STL) { thread_num = 1; } } while (0)                                         \

#endif
