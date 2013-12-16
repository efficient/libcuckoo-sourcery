/* A simple example of using the hash table that counts the
 * frequencies of a sequence of random numbers. */

#include <iostream>
#include <random>
#include <chrono>
#include <cstdint>
#include "cuckoohash_map.hh"
#include "city_hasher.hh"
#include <limits>
#include <vector>
#include <algorithm>
#include <thread>
#include <utility>

typedef uint16_t KeyType;
typedef cuckoohash_map<KeyType, size_t, CityHasher<KeyType> > Table;
const size_t thread_num = 8;
const size_t total_inserts = 10000000;

void do_inserts(Table& freq_map) {
    std::mt19937_64 gen(std::chrono::system_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<KeyType> dist(0, std::numeric_limits<KeyType>::max());
    for (size_t i = 0; i < total_inserts/thread_num; i++) {
        KeyType num = dist(gen);
        size_t count;
        if (freq_map.find(num, count)) {
            // The key is already in the table, so we increment its
            // count.
            freq_map.update(num, count+1);
        } else {
            // The key has not yet been inserted into the table, so we
            // insert it with a frequency of 1.
            freq_map.insert(num, 1);
        }
    }
}

int main() {
    Table freq_map;
    // Run the inserts in thread_num threads
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_num; i++) {
        threads.emplace_back(do_inserts, std::ref(freq_map));
    }
    for (size_t i = 0; i < thread_num; i++) {
        threads[i].join();
    }

    // We iterate through the table and print out the element with the
    // maximum number of occurrences.
    KeyType maxkey;
    size_t maxval = 0;
    for (auto it = freq_map.cbegin(); !it.is_end(); it++) {
        auto pair = *it;
        if (pair.second > maxval) {
            maxkey = pair.first;
            maxval = pair.second;
        }
    }
    
    std::cout << maxkey << " occurred " << maxval << " times." << std::endl;

    // Print some information about the table
    std::cout << "Table size: " << freq_map.size() << std::endl;
    std::cout << "Bucket count: " << freq_map.bucket_count() << std::endl;
    std::cout << "Load factor: " << freq_map.load_factor() << std::endl;
}
