/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

static size_t nt = 1;
static size_t power = 20;
static cuckoo_hashtable_t* table = NULL;
void usage() {
    printf("./bench_setsep [-p #] [-q # ] [-t #] [-h]\n");
    printf("\t-p: hash power of hash table, default %zu\n", power);
    printf("\t-t: number of threads to benchmark, default %zu\n", nt);
    printf("\t-h: usage\n");
}

typedef struct {
    int tid;
    int cpu;
} thread_param;

void* exec_thread(void* p) {
    thread_param* tp = (thread_param*) p;
    tp->cpu =  sched_getcpu();

    size_t keynum = (1 << power) * 4;
    size_t ninserted = keynum;
    double ts = time_now();

    for (size_t i = keynum * (tp->tid) + 1; i <= keynum * (tp->tid +1); i++) {
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;
        cuckoo_status st = cuckoo_insert(table, (const char*) &key, (const char*) &val);
        if (st != ok) {
            ninserted = i;
            break;
        }
    }
    double td = time_now() - ts;
    ninserted = ninserted - (keynum * (tp->tid));

    printf("[bench] %d num_inserted = %zu\n", tp->tid, ninserted);
    printf("[bench] %d insert_time = %.2f seconds\n", tp->tid, td );
    printf("[bench] %d insert_tput = %.2f MOPS\n", tp->tid, ninserted / td / MILLION);

    pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    int ch;

    while ((ch = getopt(argc, argv, "p:t:d:h")) != -1) {
        switch (ch) {
        case 'p':
            power = atoi(optarg);
            break;
        case 't':
            nt  = atoi(optarg);
            break;
        case 'h':
            usage();
            exit(-1);
        default:
            usage();
            exit(-1);
        }
    }
    size_t numkeys = (1 << power) * 4;

    printf("[bench] power = %zu\n", power);
    printf("[bench] total_keys = %zu  (%.2f M)\n", numkeys, (float) numkeys / MILLION); 
    printf("[bench] key_size = %zu bits\n", sizeof(KeyType) * 8);
    printf("[bench] value_size = %zu bits\n", sizeof(ValType) * 8);

    table = cuckoo_init(power);

    printf("[bench] inserting keys to the hash table\n");

    pthread_t threads[nt];
    thread_param tp[nt];
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    for (int i = 0; i < nt; i++) {
        tp[i].tid = i;
#ifdef __linux__
        int c = 2 * i + 1 ; //assign_core(i);
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

    for (int i = 0; i < nt; i++) {
        pthread_join(threads[i], NULL);
    }

    cuckoo_report(table);


    return 0;
}
