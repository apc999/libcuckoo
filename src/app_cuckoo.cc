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

#include <iostream>
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

int main(int argc, char** argv) 
{
    size_t power = 8;
    size_t numkeys = (1 << power) * SLOT_PER_BUCKET;

    cuckoohash_map<uint32_t, double> table(power);
    cuckoohash_map<uint32_t, double>::iterator it;

    int num_inserted = 0;
    for (int i = 1; i < numkeys; i++) {
        uint32_t key = (uint32_t) i;
        double   val = (double) 0.5 * i  - 1;

        bool done = table.insert(key, val);
        if (!done) {
            printf("inserting key %d to table fails \n", i);
            num_inserted = i;
            break;
        }
        it = table.find(key);
        if (it == table.end()) {
            assert(false);
        } else {
            std::cout << it->first << " " << it->second << endl;
        }
        
    }

    return 0;
}
