#ifndef _CUCKOOHASH_CONFIG_H
#define _CUCKOOHASH_CONFIG_H

/* 
 * number of keys per bucket at most
 */
#define SLOT_PER_BUCKET 8

/*
 * default hash table size
 */
#define HASHPOWER_DEFAULT 16

/*
 * the maximum number of cuckoo operations per insert,
 */
#define MAX_CUCKOO_COUNT 500

/*
 * the number of cuckoo paths
 */
#define NUM_CUCKOO_PATH 2

/* 
 * size of bulk cleaning 
 */
#define DEFAULT_BULK_CLEAN 1024

/* 
 * set DEBUG to 1 to enable debug output 
 */
#define DEBUG 1


#endif
