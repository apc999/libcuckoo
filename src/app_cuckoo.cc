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

#include <string>
#include <sstream>
#include <utility>
#include <stdexcept>

#include "cuckoohash_map.hh"
#include "cuckoohash_config.h" // for SLOT_PER_BUCKET

int main(int argc, char** argv) 
{
    size_t power = 8;
    size_t numkeys = (1 << power) * SLOT_PER_BUCKET;



    cuckoohash_map<std::string, double> table(power);
    std::pair<std::string, double> res;


    for (size_t i = 1; i < numkeys; i++) {
        std::stringstream ss;
        ss << "key-" << i;
        std::string key = ss.str();
        bool done = table.insert(key, (double) 0.5 * i  - 1);
        if (!done) {
            printf("inserting key %zu to table fails \n", i);
            break;
        }
        try {
            res = table.find(key);
        } catch (const std::out_of_range& e) {
            assert(false);
        }
        //std::cout << it->first << " " << it->second << std::endl;
    }

    table.report();

    return 0;
}
