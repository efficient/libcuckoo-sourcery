/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   cuckoohash.c
 * @author Bin Fan <binfan@cs.cmu.edu>
 *         Xiaozhou Li <xl@cs.princeton.edu>
 * @date   Thu Jun 20 2013
 *
 * @brief  implementation of multi-writer/multi-reader cuckoo hash
 *
 * @note   read-locked fine-grained spinlock with lock stripping
 *
 */

#include "cuckoohash.h"

/*
 * default hash table size
 */
#define HASHPOWER_DEFAULT 16

/*
 * the structure of every two buckets
 */
typedef struct {
    KeyType keys[bucketsize];
    ValType vals[bucketsize];
}  __attribute__((__packed__))
Bucket;

#define reorder_barrier() __asm__ __volatile__("" ::: "memory")
#define relax_fence() __asm__ __volatile__("pause" : : : "memory")
#define likely(x)     __builtin_expect((x), 1)
#define unlikely(x)   __builtin_expect((x), 0)

// dga does not think we need this mfence in end_incr, because
// the current code will call pthread_mutex_unlock before returning
// to the caller;  pthread_mutex_unlock is a memory barrier:
// http://www.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap04.html#tag_04_11
// __asm__ __volatile("mfence" ::: "memory");                     


static inline  uint32_t _hashed_key(const char* key) {
    return CityHash32(key, sizeof(KeyType));
}

#define hashsize(n) ((uint32_t) 1 << n)
#define hashmask(n) (hashsize(n) - 1)

#define tablesize(h)  (hashsize(h->hashpower) * sizeof(Bucket))

/**
 * @brief Compute the index of the first bucket
 *
 * @param hv 32-bit hash value of the key
 *
 * @return The first bucket
 */
static inline size_t _index_hash(cuckoo_hashtable_t* h,
                                 const uint32_t hv) {
//  return  (hv >> (32 - h->hashpower));
    return  (hv & hashmask(h->hashpower));
}


/**
 * @brief Compute the index of the second bucket
 *
 * @param hv 32-bit hash value of the key
 * @param index The index of the first bucket
 *
 * @return  The second bucket
 */
static inline size_t _alt_index(cuckoo_hashtable_t* h,
                                const uint32_t hv,
                                const size_t index) {
    // 0x5bd1e995 is the hash constant from MurmurHash2
    //uint32_t tag = hv & 0xFF;
    uint32_t tag = (hv >> 24) + 1; // ensure tag is nonzero for the multiply
    return (index ^ (tag * 0x5bd1e995)) & hashmask(h->hashpower);
}

#define TABLE_KEY(h, i, j) ((Bucket*) h->buckets)[i].keys[j]
#define TABLE_VAL(h, i, j) ((Bucket*) h->buckets)[i].vals[j]
#define V_LOCK(h, i) ((VersionType*) h->counters)[i & counter_mask] & 1
#define VERSION(h, i) ((VersionType*) h->counters)[i & counter_mask]

#define SLOT_CLEAN(h, i, j)                     \
    do {                                        \
        TABLE_KEY(h, i, j) = 0;                 \
    } while(0)

#define IS_SLOT_AVAILABLE(h, i, j) (TABLE_KEY(h, i, j) == 0)

static inline bool lock(cuckoo_hashtable_t* h, size_t i) {
    VersionType expected = VERSION(h, i);
    while(1) {
        if(!(expected & 1) &&
           __sync_bool_compare_and_swap(&VERSION(h, i), expected, expected | 1))
            break;
        relax_fence();
        expected = VERSION(h, i);
    }
    assert(!(VERSION(h, i) & 2));
    reorder_barrier();
    return V_LOCK(h, i);
}

static inline void unlock(cuckoo_hashtable_t* h, size_t i) {
    reorder_barrier();
    assert(!(VERSION(h, i) & 2));
    assert(V_LOCK(h, i));
    VERSION(h, i) = VERSION(h, i) & (~1);
    reorder_barrier();
}

static inline bool lock2(cuckoo_hashtable_t* h, size_t i1, size_t i2) {
    if ((i1 & counter_mask) < (i2 & counter_mask)) {
        lock(h, i1);
        lock(h, i2);
    }
    else if ((i2 & counter_mask) < (i1 & counter_mask)) {
        lock(h, i2);
        lock(h, i1);
    }
    else {
        lock(h, i1);
    }
    return (V_LOCK(h, i1) && V_LOCK(h, i2));
}

static inline void unlock2(cuckoo_hashtable_t* h, size_t i1, size_t i2) {
    if (likely((i1 & counter_mask) !=  (i2 & counter_mask))) {
        unlock(h, i1);
        unlock(h, i2);
    }
    else
        unlock(h, i1);
}

static inline bool is_slot_empty(cuckoo_hashtable_t* h,
                                 size_t i,
                                 size_t j) {
    if (IS_SLOT_AVAILABLE(h, i, j)) {
        return true;
    }

    if (h->expanding) {
        // when we are expanding
        // we could leave keys in their old but wrong buckets
        uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i, j));
        size_t i1 = _index_hash(h, hv);
        size_t i2 = _alt_index(h, hv, i1);

        if ((i != i1) && (i != i2)) {
            SLOT_CLEAN(h, i, j);
            return true;
        }
    }
    return false;
}

typedef struct  {
    size_t bucket;
    size_t slot;
    KeyType key;
}  __attribute__((__packed__))
CuckooRecord;

typedef struct {
    size_t bucket; //current bucket id
    int pathcode;  //path to current bucket
    int depth;     //number of cuckoo moves
} __attribute__((__packed__))
b_slot; //bucket information for BFS

// ---- implement queue functions for BFS ---
typedef struct {
    b_slot slots[MAX_CUCKOO_COUNT+1];
    int first;
    int last;
} __attribute__((__packed__))
queue;

void init_queue(queue *q)
{
    q->first = 0;
    q->last = MAX_CUCKOO_COUNT-1;

}

static void enqueue(queue *q, b_slot x)
{
    q->last = (q->last+1) % MAX_CUCKOO_COUNT;
    q->slots[ q->last ] = x;
}

static b_slot dequeue(queue *q)
{
    b_slot x = q->slots[ q->first ];
    q->first = (q->first+1) % MAX_CUCKOO_COUNT;
    return(x);
}

// --- end of queue functions ---

static b_slot _slot_search_bfs(cuckoo_hashtable_t* h,
                               size_t i1,
                               size_t i2,
                               size_t *num_kicks) {
    queue bucket_q;
    init_queue(&bucket_q);

    b_slot x1 = {.bucket=i1, .depth=0, .pathcode=1};
    enqueue(&bucket_q, x1);
    b_slot x2 = {.bucket=i2, .depth=0, .pathcode=2};
    enqueue(&bucket_q, x2);

    size_t r = (cheap_rand() >> 20) % bucketsize;

    while ((*num_kicks < MAX_CUCKOO_COUNT)) {
        b_slot x = dequeue(&bucket_q);
        size_t i = x.bucket;
        
        uint32_t hv_next = _hashed_key((char*) &TABLE_KEY(h, i, r));
        size_t bucket_child_next = _alt_index(h, hv_next, i);
        
        for (int k = 0; k < bucketsize; k++) {
            size_t j = (r+k) % bucketsize;

            size_t bucket_child = bucket_child_next;
            //uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i, j));
            //size_t bucket_child = _alt_index(h, hv, i);

            if (k < (bucketsize-1)) {
                hv_next = _hashed_key((char*) &TABLE_KEY(h, i, ((j+1)%bucketsize)));
                bucket_child_next = _alt_index(h, hv_next, i);
                __builtin_prefetch(&((Bucket*) h->buckets)[bucket_child_next]);
            }

            b_slot y = {.bucket=bucket_child, .depth=x.depth+1,\
                        .pathcode = x.pathcode * bucketsize + j};
            
            for (int m = 0; m < bucketsize; m++) {
                size_t j = (r+m) % bucketsize;
                if (is_slot_empty(h, bucket_child, j)) {
                    y.pathcode = y.pathcode * bucketsize + j;
                    return y;
                }
            }

            enqueue(&bucket_q, y);
            *num_kicks += 1;
        }
    }
    b_slot x = {.depth = -1};
    return x;
}

static int _cuckoopath_search_bfs(cuckoo_hashtable_t* h,
                                  CuckooRecord* cuckoo_path,
                                  size_t i1,
                                  size_t i2,
                                  size_t *num_kicks) {

    b_slot x = _slot_search_bfs(h, i1, i2, num_kicks);

    if (x.depth >= 0) {
        int path[MAX_BFS_DEPTH + 1];
        int num = x.pathcode;
        for(int d=0; d<=x.depth+1; d++){
            path[x.depth-d+1] = num % bucketsize;
            num = num / bucketsize;
        }
        if(path[0]==1)
            cuckoo_path[0].bucket = i1;
        else
            cuckoo_path[0].bucket = i2;
        int d = 0;
        while(1){
            CuckooRecord *curr = cuckoo_path + d;
            CuckooRecord *next = cuckoo_path + d + 1;

            size_t i = curr->bucket;
            size_t j = path[d+1];

            curr->slot = j;
            if (d==x.depth){
                break;
            }
            curr->key = TABLE_KEY(h, i, j);
            uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i, j));
            next->bucket = _alt_index(h, hv, i);
            d++;
        }
        return x.depth;
    }

    DBG("%zu max cuckoo achieved, abort\n", *num_kicks);
    return -1;
}

static int _cuckoopath_move(cuckoo_hashtable_t* h,
                            CuckooRecord* cuckoo_path,
                            size_t depth) {

    CuckooRecord *last   = cuckoo_path + depth;
    if(!is_slot_empty(h, last->bucket, last->slot)) {
        return depth;
    }

    while (depth > 0) {

        /*
         * Move the key/value in  buckets[i1] slot[j1] to buckets[i2] slot[j2]
         * and make buckets[i1] slot[j1] available
         *
         */
        CuckooRecord *from = cuckoo_path + depth - 1;
        CuckooRecord *to   = cuckoo_path + depth;
        size_t i1 = from->bucket;
        size_t j1 = from->slot;
        size_t i2 = to->bucket;
        size_t j2 = to->slot;

        lock2(h, i1, i2);

        /*
         * We plan to kick out j1, but let's check if it is still there;
         * there's a small chance we've gotten scooped by a later cuckoo.
         * If that happened, just... try again.
         */
        if (!keycmp((char*) &TABLE_KEY(h, i1, j1), (char*) &(from->key))) {
            /* try again */
            unlock2(h, i1, i2);
            return depth;
        }

        //assert(is_slot_empty(h, i2, j2));
        
        TABLE_KEY(h, i2, j2) = TABLE_KEY(h, i1, j1);
        TABLE_VAL(h, i2, j2) = TABLE_VAL(h, i1, j1);

        SLOT_CLEAN(h, i1, j1);

        unlock2(h, i1, i2);

        depth --;
    }

    return depth;

}

/**
 * @brief Try to read bucket i and check if the given key is there
 *
 * @param key The key to search
 * @param val The address to copy value to
 * @param i Index of bucket
 *
 * @return true if key is found, false otherwise
 */
static bool _try_read_from_bucket(cuckoo_hashtable_t* h,
                                  const char *key,
                                  char *val,
                                  size_t i) {
    for (size_t j = 0; j < bucketsize; j++) {

        if (keycmp((char*) &TABLE_KEY(h, i, j), key)) {
            memcpy(val, (char*) &TABLE_VAL(h, i, j), sizeof(ValType));
            return true;
        }
    }
    return false;
}

/**
 * @brief Try to add key/val to bucket i slot j
 *
 * @param key Pointer to the key to store
 * @param val Pointer to the value to store
 * @param i Bucket index
 * @param j Slot index
 *
 * @return true on success and false on failure
 */
static bool _try_add_to_slot(cuckoo_hashtable_t* h,
                             const char* key,
                             const char* val,
                             size_t i,
                             size_t j) {
    if (is_slot_empty(h, i, j)) {
        
        memcpy(&TABLE_KEY(h, i, j), key, sizeof(KeyType));
        memcpy(&TABLE_VAL(h, i, j), val, sizeof(ValType));
        
        //h->hashitems++;

        return true;
    }
    return false;
} 

/**
 * @brief Try to add key/val to bucket i
 *
 * @param key Pointer to the key to store
 * @param val Pointer to the value to store
 * @param i Bucket index
 *
 * @return true on success and false on failure
 */
static bool _try_add_to_bucket(cuckoo_hashtable_t* h,
                               const char* key,
                               const char* val,
                               size_t i) {
    for (size_t j = 0; j < bucketsize; j++) {
        if (is_slot_empty(h, i, j)) {
            
            memcpy(&TABLE_KEY(h, i, j), key, sizeof(KeyType));
            memcpy(&TABLE_VAL(h, i, j), val, sizeof(ValType));

            //h->hashitems++;
            return true;
        }
    }
    return false;
}

/**
 * @brief Try to delete key and its corresponding value from bucket i
 *
 * @param key Pointer to the key to store
 * @param i Bucket index

 * @return true if key is found, false otherwise
 */
static bool _try_del_from_bucket(cuckoo_hashtable_t* h,
                                 const char*key,
                                 size_t i) {
    for (size_t j = 0; j < bucketsize; j++) {

        if (keycmp((char*) &TABLE_KEY(h, i, j), key)) {

            SLOT_CLEAN(h, i, j);

            //h->hashitems --;
            return true;
        }
    }
    return false;
}


/**
 * @brief Internal implementation of cuckoo_find
 *
 * @param key
 * @param val
 * @param i1
 * @param i2
 *
 * @return
 */
static cuckoo_status _cuckoo_find(cuckoo_hashtable_t* h,
                                  const char *key,
                                  char *val,
                                  size_t i1,
                                  size_t i2) {
    bool result;

    lock2(h, i1, i2);

    result = _try_read_from_bucket(h, key, val, i1);
    if (!result) {
        result = _try_read_from_bucket(h, key, val, i2);
    }

    unlock2(h, i1, i2);

    if (result) {
        return ok;
    } else {
        return failure_key_not_found;
    }
}

static bool _key_in_bucket(cuckoo_hashtable_t* h,
                           const char *key,
                           size_t i1,
                           size_t i2) {
    for (size_t j = 0; j < bucketsize; j++) {
        if (keycmp((char*) &TABLE_KEY(h, i1, j), key)) {
            return true;
        }
    }
    for (size_t j = 0; j < bucketsize; j++) {
        if (keycmp((char*) &TABLE_KEY(h, i2, j), key)) {
            return true;
        }
    }
    return false;
}

static cuckoo_status _cuckoo_delete(cuckoo_hashtable_t* h,
                                    const char* key,
                                    size_t i1,
                                    size_t i2) {
    if (_try_del_from_bucket(h, key, i1)) {
        return ok;
    }

    if (_try_del_from_bucket(h, key, i2)) {
        return ok;
    }

    return failure_key_not_found;

}

static void _cuckoo_clean(cuckoo_hashtable_t* h, size_t size) {
    for (size_t ii = 0; ii < size; ii++) {
        size_t i = h->cleaned_buckets;
        for (size_t j = 0; j < bucketsize; j++) {
            if (IS_SLOT_AVAILABLE(h, i, j) == 0) {
                continue;
            }
            uint32_t hv = _hashed_key((char*) &TABLE_KEY(h, i, j));
            size_t i1 = _index_hash(h, hv);
            size_t i2 = _alt_index(h, hv, i1);
            if ((i != i1) && (i != i2)) {
                //DBG("delete key %u , i=%zu i1=%zu i2=%zu\n", TABLE_KEY(h, i, j), i, i1, i2);
                SLOT_CLEAN(h, i, j);
            }
        }
        h->cleaned_buckets++;
        if (h->cleaned_buckets == hashsize((h->hashpower))) {
            h->expanding = false;
            DBG("table clean done, cleaned_buckets = %zu\n", h->cleaned_buckets);
            return;
        }
    }
    //DBG("_cuckoo_clean: cleaned_buckets = %zu\n", h->cleaned_buckets);
}


/********************************************************************
 *               Interface of cuckoo hash table
 *********************************************************************/

cuckoo_hashtable_t* cuckoo_init(const int hashtable_init) {
    cuckoo_hashtable_t* h = (cuckoo_hashtable_t*) malloc(sizeof(cuckoo_hashtable_t));
    if (!h) {
        goto Cleanup;
    }

    h->expanding  = false;
    pthread_mutex_init(&h->lock, NULL);

    if (hashtable_init != -1) {
        h->hashpower  = (hashtable_init > 0) ? hashtable_init : HASHPOWER_DEFAULT;
        h->hashitems  = 0;
        h->buckets = malloc(tablesize(h));
        if (! h->buckets) {
            fprintf(stderr, "Failed to init hashtable.\n");
            goto Cleanup;
        }
        memset(h->buckets, 0, tablesize(h));
    }

    //read from file
    else{
        FILE *fp;
        fp = fopen("hashtable", "r");

        size_t hashpower;
        fseek(fp, SEEK_SET, 0);
        if(!fread(&hashpower, sizeof(size_t), 1, fp)) goto Cleanup;
        h->hashpower = hashpower;

        h->buckets = malloc(tablesize(h));
        if (! h->buckets) {
            fprintf(stderr, "Failed to init hashtable.\n");
            goto Cleanup;
        }

        fseek(fp, SEEK_SET, sizeof(size_t));
        if(!fread(&h->hashitems, sizeof(size_t), 1, fp)) goto Cleanup;

        fseek(fp, SEEK_SET, 2 * sizeof(size_t));
        if(!fread(h->buckets, tablesize(h), 1, fp)) goto Cleanup;

        fclose(fp);
    }

    h->counters = malloc(counter_size * sizeof(VersionType));
    if (! h->counters) {
        fprintf(stderr, "Failed to init counter array.\n");
        goto Cleanup;
    }
    memset(h->counters, 0, counter_size * sizeof(VersionType));

    return h;

Cleanup:
    if (h) {
        free(h->counters);
        free(h->buckets);
    }
    free(h);
    return NULL;

}

cuckoo_status cuckoo_exit(cuckoo_hashtable_t* h) {
    pthread_mutex_destroy(&h->lock);
    free(h->buckets);
    free(h->counters);
    free(h);
    return ok;
}

cuckoo_status cuckoo_find(cuckoo_hashtable_t* h,
                          const char *key,
                          char *val) {

    uint32_t hv    = _hashed_key(key);
    size_t i1      = _index_hash(h, hv);
    size_t i2      = _alt_index(h, hv, i1);

    cuckoo_status st = _cuckoo_find(h, key, val, i1, i2);

    if (st == failure_key_not_found) {
        //DBG("miss for key %u i1=%zu i2=%zu hv=%u\n", *((KeyType*) key), i1, i2, hv);
    }

    return st;
}

cuckoo_status cuckoo_insert(cuckoo_hashtable_t* h,
                            const char *key,
                            const char* val) {

    uint32_t hv = _hashed_key(key);
    size_t i1   = _index_hash(h, hv);
    size_t i2   = _alt_index(h, hv, i1);
    cuckoo_status st;

    for (size_t j = 0; j < bucketsize; j++) {
        if (is_slot_empty(h, i1, j)) {
            lock2(h, i1, i2);
            if (_key_in_bucket(h, key, i1, i2)) {
                unlock2(h, i1, i2);
                return failure_key_duplicated;
            }                

            if(_try_add_to_slot(h,key,val,i1,j)){
                if (h->expanding) {
                    _cuckoo_clean(h, DEFAULT_BULK_CLEAN);
                }
                unlock2(h, i1, i2);
                //printf("cuckoo path length: 0\n");
                return ok;
            }
            unlock2(h, i1, i2);
            break;
        }
    }    

    for (size_t j = 0; j < bucketsize; j++) {
        if (is_slot_empty(h, i2, j)) {
            lock2(h, i1, i2);
            if (_key_in_bucket(h, key, i1, i2)) {
                unlock2(h, i1, i2);
                return failure_key_duplicated;
            }                
            if(_try_add_to_slot(h,key,val,i2,j)){
                if (h->expanding) {
                    _cuckoo_clean(h, DEFAULT_BULK_CLEAN);
                }
                unlock2(h, i1, i2);
                return ok;
            }
            unlock2(h,i1, i2);
            break;
        }
    }
    
    static __thread CuckooRecord* cuckoo_path = NULL;
    if (!cuckoo_path) { 
        cuckoo_path = malloc(MAX_BFS_DEPTH * sizeof(CuckooRecord));
        if (!cuckoo_path) {
            fprintf(stderr, "Failed to init cuckoo path.\n");
            return failure;
        }
    }

    while(1){
        size_t num_kicks = 0;
        int depth = _cuckoopath_search_bfs(h, cuckoo_path, i1, i2, &num_kicks);
        if (depth < 0)
            break;

        int curr_depth = _cuckoopath_move(h, cuckoo_path, depth);
        if (curr_depth == 0) {
            //printf("cuckoo path length: %d\n", depth);
            lock2(h, i1, i2);
            if (_key_in_bucket(h, key, i1, i2)) {
                unlock2(h, i1, i2);
                return failure_key_duplicated;
            }

            size_t i = cuckoo_path[0].bucket;
            size_t j = cuckoo_path[0].slot;
            if (_try_add_to_slot(h, key, val, i, j)) {
                st = ok;
            }
            if (h->expanding) {
                //
                // still some work to do before releasing the lock
                //
                _cuckoo_clean(h, DEFAULT_BULK_CLEAN);
            }
            unlock2(h, i1, i2);
            break;
        }
    }
    if(st != ok) {      
        DBG("hash table is full (hashpower = %zu), need to increase hashpower\n",
            h->hashpower);
        st = failure_table_full;
    }
    return st;
}

cuckoo_status cuckoo_delete(cuckoo_hashtable_t* h,
                            const char *key) {

    uint32_t hv = _hashed_key(key);
    size_t i1   = _index_hash(h, hv);
    size_t i2   = _alt_index(h, hv, i1);

    lock2(h, i1, i2);
    cuckoo_status st = _cuckoo_delete(h, key, i1, i2);
    unlock2(h, i1, i2);

    return st;
}

cuckoo_status cuckoo_expand(cuckoo_hashtable_t* h) {

    mutex_lock(&h->lock);
    if (h->expanding) {
        mutex_unlock(&h->lock);
        //DBG("expansion is on-going\n", NULL);
        return failure_under_expansion;
    }
    
    h->expanding = true;

    Bucket* old_buckets = (Bucket*) h->buckets;
    Bucket* new_buckets = (Bucket*) malloc(tablesize(h) * 2);
    if (!new_buckets) {
        h->expanding = false;
        mutex_unlock(&h->lock);
        return failure_space_not_enough;
    }


    memcpy(new_buckets, h->buckets, tablesize(h));
    memcpy(new_buckets + tablesize(h), h->buckets, tablesize(h));

    h->buckets = new_buckets;
    h->hashpower++;
    h->cleaned_buckets = 0;

    //h->expanding = false;
    //_cuckoo_clean(h, hashsize(h->hashpower));

    mutex_unlock(&h->lock);

    free(old_buckets);

    return ok;
}

void cuckoo_report(cuckoo_hashtable_t* h) {

    DBG("total number of items %zu\n", h->hashitems);
    DBG("total size %zu Bytes, or %.2f MB\n", \
        tablesize(h), (float) tablesize(h) / (1 <<20));
    DBG("load factor %.4f\n", 1.0 * h->hashitems / bucketsize / hashsize(h->hashpower));
}

void cuckoo_dump(cuckoo_hashtable_t* h) {

    FILE *fp;
    fp = fopen("hashtable", "w");
    size_t hashpower = h->hashpower;
    fwrite(&hashpower, sizeof(size_t), 1, fp);
    fwrite(&h->hashitems, sizeof(size_t), 1, fp);
    fwrite(h->buckets, tablesize(h), 1, fp);
    fclose(fp);
}


float cuckoo_loadfactor(cuckoo_hashtable_t* h) {
    return 1.0 * h->hashitems / bucketsize / hashsize(h->hashpower);
}
