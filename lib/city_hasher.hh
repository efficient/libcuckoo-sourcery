#include "city.h"

/*! CityHasher is a std::hash-style wrapper around cityhash. We
 *  encourage using CityHash over the std::hash function if
 *  possible. */
template <class Key>
class CityHasher {
public:
    size_t operator()(const Key& k) const {
        return CityHash64((const char*) &k, sizeof(k));
    }
};

/*! This is a template specialization of CityHasher for
 *  std::string. */
template <>
class CityHasher<std::string> {
public:
    size_t operator()(const std::string& k) const {
        return CityHash64(k.c_str(), k.size());
    }
};
