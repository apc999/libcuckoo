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


#include "cuckoohash_map.h"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET

typedef uint32_t KeyType;
typedef uint32_t ValType;

int main(int argc, char** argv)
{
    bool passed = true;
    int i;
    size_t power = 19;
    size_t numkeys = (1 << power) * SLOT_PER_BUCKET;

    printf("initializing two hash tables\n");

    cuckoohash_map<KeyType, ValType> smalltable(power);
    cuckoohash_map<KeyType, ValType> bigtable(power + 1);

    printf("inserting keys to the hash table\n");
    int failure = -1;
    for (i = 1; i < numkeys; i++) {
        bool ret;
        KeyType key = (KeyType) i;
        ValType val = (ValType) i * 2 - 1;

        if (failure == -1) {
            ret = smalltable.insert(key, val);
            if (!ret) {
                printf("inserting key %d to smalltable fails \n", i);
                failure = i;
            }
        }
        ret = bigtable.insert(key, val);
        if (!ret) {
            printf("inserting key %d to bigtable fails \n", i);
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
                printf("failure to read key %d from smalltable\n", i);
                passed = false;
                break;
            }
            if (val1 != i * 2 -1 ) {
                printf("smalltable reads wrong value for key %d\n", i);
                passed = false;
                break;
            }
        }
        else {
            if (ret1) {
                printf(" key %d should not be in smalltable\n", i);
                passed = false;
                break;
            }
        }


        if (!ret2) {
            printf("failure to read key %d from bigtable\n", i);
            passed = false;
            break;
        }

        if (val2 != i * 2 -1 ) {
            printf("bigtable reads wrong value for key %d\n", i);
            passed = false;
            break;
        }
    }

    smalltable.report();
    bigtable.report();

    printf("[%s]\n", passed ? "PASSED" : "FAILED");

    return 0;
}
