/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   bench_cuckoo_rw.cc
 *
 * @brief  throughput benchmark: concurrent multiple reads/writes.
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
#include <unistd.h>
#include <math.h>
#include <sys/time.h>
#include <algorithm>

extern "C" {
#include "cuckoohash.h"
}


using namespace std;

#define MILLION 1000000

double time_now()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return (double)tv.tv_sec + (double)tv.tv_usec / MILLION;
}

static size_t nt    = 1;
static size_t power = 20;
static double w_ratio = 0.1;
static size_t nr = 1 << 25;
static cuckoo_hashtable_t* table = NULL;

static double load_factor = 0.5;

void usage() {
    printf("./bench_setsep [-p #] [-t #] [-r #] [-h]\n");
    printf("\t-p: hash power of hash table, default %zu\n", power);
    printf("\t-t: number of threads to benchmark, default %zu\n", nt);
    printf("\t-r: write ratio of each thread, default %.2f\n", w_ratio);
    printf("\t-h: usage\n");
}

#define CACHE_LINE_SIZE 64

typedef struct {
    int     tid;
    double  time;
    double  tput;
    size_t  gets;
    int     cpu;
} __attribute__((aligned (CACHE_LINE_SIZE))) thread_param;


void* exec_thread(void* p) {
    thread_param* tp = (thread_param*) p;
    tp->time = 0;  
    tp->gets = 0;
    tp->tput = 0;
    tp->cpu =  sched_getcpu();

    size_t numkeys_inserted = (1 << power) * bucketsize * load_factor;
    size_t numkeys_read = numkeys_inserted / nt;
    size_t numkeys_read_start = numkeys_read * tp->tid + 1;
    size_t numkeys_read_end = numkeys_read * (tp->tid + 1);

    std::mt19937_64 rng;
    rng.seed(123456);
    size_t i_r = rng() % numkeys_read;
    
    double ts = time_now();
    for(size_t j = 0; j < numkeys_read; j++) {
        i_r = (i_r + 1) % (numkeys_read);
        KeyType key = (KeyType) (i_r + numkeys_read_start);
        ValType val;
        cuckoo_status st  = cuckoo_find(table, (const char*) &key, (char*) & val);
        if (st != ok) {
            printf("[bench] %d read fails, key %d\n", tp->tid, key);
        }
        //else if (val != key*2-1) {
        //    printf("[bench] %d read wrong value: key %d value %d\n", tp->tid, key, val);
            //break;
        //}
        tp->gets++;
    }

    tp->time = time_now() - ts;
    tp->tput = (float) (tp->gets) / tp->time;
    
    //printf("[bench] %d num_inserted = %zu\n", tp->tid, tp->puts);
    //printf("[bench] %d num_lookup = %zu\n", tp->tid, tp->gets);
    //printf("[bench] %d execute_time = %.2f seconds\n", tp->tid, tp->time );
    printf("[bench] %d request_tput = %.2f MOPS\n", tp->tid, tp->tput / MILLION);
    
    pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    int ch;

    while ((ch = getopt(argc, argv, "p:t:r:h")) != -1) {
        switch (ch) {
        case 'p':
            power = atoi(optarg);
            break;
        case 't':
            nt  = atoi(optarg);
            break;
        case 'r':
            w_ratio  = atof(optarg);
            break;
        case 'h':
            usage();
            exit(-1);
        default:
            usage();
            exit(-1);
        }
    }

    //for(int i=0; i<87; i++)
    //    load_factor[i] = 0.1 + i*0.01;

    size_t totalkeys = (1 << power) * bucketsize;
    size_t numkeys = totalkeys * load_factor;
    
    printf("[bench] power = %zu\n", power);
    printf("[bench] total_keys = %zu  (%.2f M)\n", totalkeys, (float) totalkeys / MILLION); 
    printf("[bench] key_size = %zu bits\n", sizeof(KeyType) * 8);
    printf("[bench] value_size = %zu bits\n", sizeof(ValType) * 8);
    printf("------------------------------\n");
    
    table = cuckoo_init(power);
    
    printf("[bench] inserting keys to the hash table\n");

    size_t ninserted = numkeys;

    double ts = time_now();

    for (size_t i = 1; i <= numkeys; i++) {
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;
        cuckoo_status st = cuckoo_insert(table, (const char*) &key, (const char*) &val);
        if (st != ok) {
            ninserted = i;
            break;
        }
    }

    //cuckoo_dump(table);

    /*
    double td = time_now() - ts;
    
    printf("[bench] num_inserted = %zu\n", ninserted );
    printf("[bench] insert_time = %.2f seconds\n", td );
    printf("[bench] insert_tput = %.2f MOPS\n", ninserted / td / MILLION);

    cuckoo_report(table);
    */

    printf("[bench] concurrent reads in hash table\n");
    printf("[bench] load factor %.3f\n",load_factor);

    pthread_t threads[nt];
    thread_param tp[nt];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
        
    for (int i = 0; i < nt; i++) {
        tp[i].tid = i;
#ifdef __linux__
        int c = i; //2 * i + 1 ; //assign_core(i);
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(c, &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
#endif
        int rc = pthread_create(&threads[i],  &attr, exec_thread, (void*) &(tp[i]));
        if (rc) {
            perror("error, pthread_create\n");
            exit(-1);
        }
    }
    
    double total_tput = 0.0;
    for (int i = 0; i < nt; i++) {
        pthread_join(threads[i], NULL);
        total_tput += tp[i].tput;
    }
        
    printf("[bench] read_tput = %.3lf MOPS\n", (double) total_tput / MILLION);
        
    cuckoo_report(table);
    cuckoo_exit(table);

    return 0;
}
