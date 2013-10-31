#ifndef _CUCKOOHASH_CONFIG_H
#define _CUCKOOHASH_CONFIG_H
#include <stdint.h>

typedef uint32_t KeyType;
typedef uint32_t ValType;
typedef uint32_t VersionType;

#define  counter_size  ((VersionType)1 << (16))
#define  counter_mask  (counter_size - 1)

/*
 * number of slots per bucket
 */
#define bucketsize 8

/*
 * The maximum number of cuckoo operations per insert,
 */
#define MAX_CUCKOO_COUNT 800

/*
 * The max length of the cuckoo path for BFS
 * bucketsize ^ MAX_BFS_DEPTH > MAX_CUCKOO_COUNT / 2
 */
#define MAX_BFS_DEPTH 4

/*
 * The number of cuckoo paths for DFS
 */
#define NUM_CUCKOO_PATH 2


/* size of bulk cleaning */
#define DEFAULT_BULK_CLEAN 1024


/* set DEBUG to 1 to enable debug output */
#define DEBUG 1


#endif
