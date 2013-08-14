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

typedef uint32_t KeyType;
typedef uint32_t ValType;


using namespace std;

#define MILLION 1000000

double time_now()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return (double)tv.tv_sec + (double)tv.tv_usec / MILLION;
}

static size_t nq    = 1024; 
static size_t nt    = 1;
static size_t power = 21;
static float  duration = 2.0;
static cuckoo_hashtable_t* table = NULL;
void usage() {
    printf("./bench_setsep [-p #] [-q # ] [-t #] [-h]\n");
    printf("\t-p: hash power of hash table, default %zu\n", power);
    printf("\t-q: number of queries = 2^(arg), default 10\n");
    printf("\t-t: number of threads to benchmark, default %zu\n", nt);
    printf("\t-h: usage\n");
}

typedef struct {
    int     tid;
    double  time;
    double  tput;
    size_t  gets;
    size_t  puts;
    size_t  hits;
    size_t* queries;
    int cpu;
    uint32_t junk;
} thread_param;

void* exec_thread(void* p) {
    thread_param* tp = (thread_param*) p;
    tp->time = 0;  
    tp->hits = 0;
    tp->gets = 0;
    tp->puts = 0;
#ifdef __linux__
    tp->cpu =  sched_getcpu();
#endif
    size_t   w     = nq / nt;
    size_t*  q     = tp->queries + w * tp->tid;
    size_t   total = 0;
    size_t   left  = w; 
    size_t   k     = 0;

    while ((tp->time < duration) && (left > 0)) {
        size_t step = (left >= 1000000) ? 1000000 : left;
        double ts = time_now();
        for (size_t i = 0; i < step; i++, k++) {
            KeyType key = (KeyType) q[k];
            ValType  val;
            cuckoo_status st  = cuckoo_find(table, (const char*) &key, (char*) & val);
            tp->junk ^= val;
        }
        tp->time += time_now() - ts;
        left = left - step;
    }


    tp->tput = (float) k / tp->time;
    pthread_exit(NULL);
}

int main(int argc, char **argv)
{



    int ch;

    while ((ch = getopt(argc, argv, "p:q:t:d:h")) != -1) {
        switch (ch) {
        case 'p':
            power = atoi(optarg);
            break;
        case 'q':
            nq  = 1 << atoi(optarg);
            break;
        case 't':
            nt  = atoi(optarg);
            break;
        case 'd':
            duration = atof(optarg);
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

    table = cuckoo_init(power, sizeof(KeyType), sizeof(ValType));

    printf("[bench] inserting keys to the hash table\n");

    size_t ninserted = numkeys;

    double ts, td;

    ts = time_now();

    for (size_t i = 1; i < numkeys; i++) {
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;
        cuckoo_status st = cuckoo_insert(table, (const char*) &key, (const char*) &val);
        if (st != ok) {
            ninserted = i;
            break;
        }
    }

    td = time_now() - ts;

    printf("[bench] num_inserted = %zu\n", ninserted );
    printf("[bench] insert_time = %.2f seconds\n", td );
    printf("[bench] insert_tput = %.2f MOPS\n", ninserted / td / MILLION);

    cuckoo_report(table);

    std::mt19937_64 rng;
    //rng.seed(static_cast<unsigned int>(std::time(0)));
    rng.seed(123456);

    size_t* queries = new size_t[nq];
    for (size_t i = 0; i < nq; i++) {
        queries[i] = rng() % ninserted;
    }

    printf("[bench] looking up keys in the hash table\n");

    pthread_t threads[nt];
    thread_param tp[nt];
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    for (int i = 0; i < nt; i++) {
        tp[i].tid = i;
        tp[i].queries = queries;
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
    double total_tput = 0.0;
    size_t total_puts = 0;
    uint32_t junk = 0;
    for (int i = 0; i < nt; i++) {
        pthread_join(threads[i], NULL);
        total_tput += tp[i].tput;
        junk ^= tp[i].junk;
        //if (verbose) 
        {
            printf("[thread%d] %.2f sec, cpu %zu, tput %.2f MOPS\n", 
                   i, tp[i].time, tp[i].cpu, tp[i].tput / MILLION);
        }
    }

    printf("[bench] num_queries = %zu (%.2f M)\n", nq, (float) nq / MILLION);
    printf("[bench] lookup_time = %.4lf seconds\n", td);
    printf("[bench] lookup_tput = %.4lf M items / seconds\n", (double) total_tput / MILLION);
    printf("[bench] ignore this line %u\n", junk);

    return 0;
}
