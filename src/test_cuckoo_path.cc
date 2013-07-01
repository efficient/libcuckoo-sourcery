/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   test_cuckoo_path.c
 * 
 * @brief  a script to print cuckoo path length
 *  
 */
#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <math.h>


extern "C" {
#include "cuckoohash.h"
}

int main(int argc, char** argv) 
{
    bool passed = true;
    int i=0;    
    size_t power = 20;
    size_t numkeys = (1 << power) * bucketsize;

    printf("number of keys: %zu\n",numkeys);

    printf("initializing hash tables\n");

    cuckoo_hashtable_t* hashtable = cuckoo_init(power);

    printf("inserting keys to the hash table\n");
    int failure = -1;
    //for (i = 1; i < numkeys; i++) {
    while(failure == -1) {
        i++;
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;
        cuckoo_status st;

        if (failure == -1) {
            st = cuckoo_insert(hashtable, (const char*) &key, (const char*) &val);
            if (st != ok) {
                printf("inserting key %d to hashtable fails \n", i);
                failure = i;
            }
        }
    }

    /*
    printf("looking up keys in the hash table\n");
    for (i = 1; i < numkeys; i++) {
        ValType val;
        KeyType key = (KeyType) i;
        cuckoo_status st = cuckoo_find(hashtable, (const char*) &key, (char*) &val);
        if (i < failure) {
            if (st != ok) {
                printf("failure to read key %d from hashtable\n", i);
                passed = false;
                break;
            }
            if (val != i * 2 -1 ) {
                printf("hashtable reads wrong value for key %d\n", i);
                passed = false;
                break;
            }
        }

    }
    */

    cuckoo_report(hashtable);
    cuckoo_exit(hashtable);

    // printf("[%s]\n", passed ? "PASSED" : "FAILED");

    return 0;
}
