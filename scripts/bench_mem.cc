/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <iostream>
//#include <random>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sched.h>
#include <pthread.h>
#include <cstdlib>

using namespace std;

//static mt19937  engine;  
static char*    buf = NULL;
static uint32_t indexmask = 0;

static const size_t kNumIters        = 100000000;
static const size_t kSizeStride      = 2048;
static const size_t kMillion         = 1000000;

static size_t len = 8 * (1 << 20); // size of memory table, by default 8 MB


static double timeval_diff(const struct timeval * const start, 
                    const struct timeval * const end) {
    /* Calculate the second difference*/
    double r = end->tv_sec - start->tv_sec;

    /* Calculate the microsecond difference */
    if (end->tv_usec > start->tv_usec)
        r += (end->tv_usec - start->tv_usec)/1000000.0;
    else if (end->tv_usec < start->tv_usec)
        r -= (start->tv_usec - end->tv_usec)/1000000.0;

    return r;
}

/*
 * Init the memory table, fill the table with random numbers
 */
static void init_table(const size_t len)
{
    printf("init a table of %.2f MB\n",  (double) len /1024/1024);
    printf("indexmask=0x%x\n", indexmask);

    buf = new char[len];

    int res = mlock(buf, len);
    if (res != 0) {
        cout << "mlock failed, run with sudo to ensure mlock\n";
    }

    for (size_t i = 0; i < len / sizeof(uint32_t); i++) {
        ((uint32_t*) buf)[i] = rand(); //engine();
    }
}

/*
 * Release the memory used for the table
 */
static void free_table(size_t len)
{
    if (buf) {
        free(buf);
        munlock(buf, len);
    }
}

/*
 * Working routine for benchmark
 */
static void benchmark() {
    struct timeval tv_s, tv_e;

    uint64_t x;

    gettimeofday(&tv_s, NULL);

    for (size_t i = 0; i < kNumIters; i++) {

        uint64_t *p = (uint64_t*) (buf + (x & indexmask));

        for (size_t j = 0; j < kSizeStride / sizeof(uint64_t); j++) {
            x ^= p[j];
        }
        x ^= i;
    }

    gettimeofday(&tv_e, NULL);

    double time         = timeval_diff(&tv_s, &tv_e);
    double tput_strides = (double) kNumIters / time;
    double tput_bytes   = (double) kNumIters * kSizeStride / time; 

    printf("stride size: %zu bytes\n", kSizeStride);
    printf("tput_strides: %.2f Mops per sec\n", tput_strides / kMillion);
    printf("tput_bytes:   %.2f MB per sec\n", tput_bytes / (1 << 20) );
    printf("junk: %x\n", (unsigned int) x);
}

static void usage() {
    cerr << "./bench_mem [-p #] [-h]" <<endl;
    cerr << "\t-p [#]:  the size of in-memory is 1 << # bytes" << endl;
    cerr << "\t-h:     show usage" << endl;
}

int main(int argc, char** argv) {

    char ch;
    while ((ch = getopt(argc, argv, "p:h")) != -1) {
        switch (ch) {
        case 'p':
            len = 1 << (20 + atoi(optarg));
            indexmask = len - 1;
            break;
        case 'h': usage(); exit(0); break;
        default:
            usage();
            exit(-1);
        }
    }

    init_table(len);
    benchmark();
    free_table(len);
}
