/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/**
 * @file   test_cuckoo.c
 * @author Bin Fan <binfan@cs.cmu.edu>
 * @date   Thu Feb 28 15:54:47 2013
 *
 * @brief  a simple example of using cuckoo hash table
 *
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
#include <stdint.h>


#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET

typedef uint32_t KeyType;
typedef uint32_t ValType;

int main(int argc, char** argv)
{
    bool passed = true;
    size_t i;
    size_t power = 19;
    size_t numkeys = (1 << power) * SLOT_PER_BUCKET;

    printf("initializing two hash tables\n");

    cuckoohash_map<KeyType, ValType> smalltable(power);
    cuckoohash_map<KeyType, ValType> bigtable(power + 1);

    printf("inserting keys to the hash table\n");
    size_t failure = numkeys;
    for (i = 1; i < numkeys; i++) {
        bool ret;
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;

        if (failure == numkeys) {
            ret = smalltable.insert(key, val);
            if (!ret) {
                printf("inserting key %zu to smalltable fails \n", i);
                failure = i;
            }
        }
        ret = bigtable.insert(key, val);
        if (!ret) {
            printf("inserting key %zu to bigtable fails \n", i);
            break;
        }
    }

    printf("looking up keys in the hash table\n");
    for (i = 1; i < numkeys; i++) {
        bool ret1, ret2;
        ValType val1, val2;
        KeyType key = (KeyType) i;

        ret1 = smalltable.find(key, val1);
        ret2 = bigtable.find(key, val2);

        if (i < failure) {
            if (!ret1) {
                printf("failure to read key %zu from smalltable\n", i);
                passed = false;
                break;
            }
            if (val1 != i * 2 -1 ) {
                printf("smalltable reads wrong value for key %zu\n", i);
                passed = false;
                break;
            }
        }
        else {
            if (ret1) {
                printf(" key %zu should not be in smalltable\n", i);
                passed = false;
                break;
            }
        }


        if (!ret2) {
            printf("failure to read key %zu from bigtable\n", i);
            passed = false;
            break;
        }

        if (val2 != i * 2 -1 ) {
            printf("bigtable reads wrong value for key %zu\n", i);
            passed = false;
            break;
        }
    }

    printf("total number of items %zu\n", smalltable.size());
    printf("load factor %f\n", smalltable.load_factor());

    printf("total number of items %zu\n", bigtable.size());
    printf("load factor %f\n", bigtable.load_factor());

    printf("[%s]\n", passed ? "PASSED" : "FAILED");

    return 0;
}
