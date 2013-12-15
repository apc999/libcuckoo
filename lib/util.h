#ifndef UTIL_H
#define UTIL_H

#include <stdint.h>
#include <pthread.h>
#include <pthread.h>

#include "cuckoohash_config.h" // for DEBUG

namespace cuckoohash_map {

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"

#if DEBUG
#  define DBG(fmt, args...)  fprintf(stderr, ANSI_COLOR_GREEN"[libcuckoo:%s:%d:%lu] " fmt"" ANSI_COLOR_RESET,__FILE__,__LINE__, (unsigned long)pthread_self(), args)
#else
#  define DBG(fmt, args...)  do {} while (0)
#endif
}  // namespace cuckoohash_map

#endif
