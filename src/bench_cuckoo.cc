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



void usage() {
    printf("./bench_setsep [-p #] [-q # ] [-t #] [-h]\n");
    printf("\t-p: hash power of hash table, default 19\n");
    printf("\t-q: number of queries = 2^(arg), default 10\n");
    printf("\t-t: number of threads to create setsep, default 1\n");
    printf("\t-h: usage\n");
}

int main(int argc, char **argv)
{


    size_t nq    = 1024; 
    size_t nt    = 1;
    size_t power = 19;

    int ch;

    while ((ch = getopt(argc, argv, "p:q:t:h")) != -1) {
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

    cuckoo_hashtable_t* table = cuckoo_init(power);

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
    uint32_t junk = 0;
    ts = time_now();

    for (size_t i = 0; i < nq; i++) {
        KeyType key = (KeyType) queries[i];
        ValType  val;

        cuckoo_status st  = cuckoo_find(table, (const char*) &key, (char*) & val);
        junk ^= val;
    }
    td = time_now() - ts;

    printf("[bench] num_queries = %zu (%.2f M)\n", nq, (float) nq / MILLION);
    printf("[bench] lookup_time = %.4lf seconds\n", td);
    printf("[bench] lookup_tput = %.4lf M items / seconds\n", (double) nq / td / MILLION);
    printf("[bench] ignore this line %u\n", junk);

    return 0;
}
