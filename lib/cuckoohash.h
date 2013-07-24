#ifndef _CUCKOOHASH_H
#define _CUCKOOHASH_H

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "config.h"
#include "cuckoohash_config.h"
#include "city.h"
#include "util.h"
#include "tsx.h"

typedef enum {
    ok = 0,
    failure = 1,
    failure_key_not_found = 2,
    failure_key_duplicated = 3,
    failure_space_not_enough = 4,
    failure_function_not_supported = 5,
    failure_table_full = 6,
    failure_under_expansion = 7,
    failure_path_invalid = 8,
} cuckoo_status;

#define  counter_size  ((uint64_t)1 << (18))
#define  counter_mask  (counter_size - 1)

/*
 * number of slots per bucket
 */
#define bucketsize 8

/*
 * The maximum number of cuckoo operations per insert,
 */
#define MAX_CUCKOO_COUNT 250

/*
 * The max length of the cuckoo path for BFS
 * bucketsize ^ MAX_BFS_DEPTH > MAX_CUCKOO_COUNT / 2
 */
#define MAX_BFS_DEPTH 4

/*
 * The number of cuckoo paths for DFS
 */
#define NUM_CUCKOO_PATH 2

/*
 * the structure of a buckoo hash table
 */
typedef struct {

  /* an array of version counters */
  void* counters;
    
  /* pointer to the array of buckets */
  void*  buckets;
  
  /* the mutex to serialize insert, delete, expand */
  pthread_mutex_t lock;

  /* number of items inserted */
  size_t hashitems;
  
  /* 2**hashpower is the number of buckets */
  volatile size_t hashpower;

  /* number of buckets has been cleaned */
  size_t cleaned_buckets;
  
  /* denoting if the table is doing expanding */
  bool expanding;
  
} cuckoo_hashtable_t;



/** 
 * @brief Initialize the hash table
 * 
 * @param h handler to the hash table
 * @param hashtable_init The logarithm of the initial table size
 *
 * @return handler to the hashtable on success, NULL on failure
 */
cuckoo_hashtable_t* cuckoo_init(const int hashpower_init);

/** 
 * @brief Cleanup routine
 * 
 */
cuckoo_status cuckoo_exit(cuckoo_hashtable_t* h);


/** 
 * @brief Lookup key in the hash table
 * 
 * @param h handler to the hash table
 *
 * @param key key to search 
 * @param val value to return
 * 
 * @return ok if key is found, not_found otherwise
 */
cuckoo_status cuckoo_find(cuckoo_hashtable_t* h, const char *key, char *val);



/** 
 * @brief Insert key/value to cuckoo hash table
 * 
 *  Inserting new key/value pair. 
 *  If the key is already inserted, the new value will not be inserted
 *
 *
 * @param h handler to the hash table
 * @param key key to be inserted
 * @param val value to be inserted
 * 
 * @return ok if key/value are succesfully inserted
 */
cuckoo_status cuckoo_insert(cuckoo_hashtable_t* h, const char *key, const char* val);


/** 
 * @brief Delete key/value from cuckoo hash table
 * 
 * @param h handler to the hash table
 * @param key key to be deleted
 *
 * @return ok if key is succesfully deleted, not_found if the key is not present
 */
cuckoo_status cuckoo_delete(cuckoo_hashtable_t* h, const char *key);


/** 
 * @brief Grow the hash table to the next power of 2
 * 
 * @param h handler to the hash table
 *
 * @return ok if table is succesfully expanded, not_enough_space if no space to expand
 */
cuckoo_status cuckoo_expand(cuckoo_hashtable_t* h);


/** 
 * @brief Print stats of this hash table
 * 
 * @param h handler to the hash table
 * 
 * @return Void
 */
void cuckoo_report(cuckoo_hashtable_t* h);


/**
 * @brief Dump hash table to a file named "hashtable"
 *
 * @param h handler to the hash table
 * 
 * @return Void
 */
void cuckoo_dump(cuckoo_hashtable_t* h);


/** 
 * @brief Return the load factor of this hash table
 * 
 * @param h handler to the hash table
 * 
 * @return load factor
 */
float cuckoo_loadfactor(cuckoo_hashtable_t* h);

#endif
