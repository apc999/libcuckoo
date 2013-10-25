// For parsing command line flags during tests
#include <iostream>
#include <array>

#include <cstring>
#include <cstdlib>
#include <cerrno>

// Parses boolean flags and flags with positive integer arguments
void parse_flags(int argc, char**argv,
                 const char* args[], size_t* arg_vars[], const char* arg_help[], size_t arg_num,
                 const char* flags[], bool* flag_vars[], const char* flag_help[], size_t flag_num) {
    errno = 0;
    for (int i = 0; i < argc; i++) {
        for (size_t j = 0; j < arg_num; j++) {
            if (strcmp(argv[i], args[j]) == 0) {
                if (i == argc-1) {
                    std::cerr << "You must provide a positive integer argument after the " << args[j] << " argument" << std::endl;
                    exit(1);
                } else {
                    size_t argval = strtoull(argv[i+1], NULL, 10);
                    if (errno != 0) {
                        std::cerr << "The argument to " << args[j] << " must be a valid size_t" << std::endl;
                        exit(1);
                    } else {
                        *(arg_vars[j]) = argval;
                    }
                }
            }
        }
        for (size_t j = 0; j < flag_num; j++) {
            if (strcmp(argv[i], flags[j]) == 0) {
                *(flag_vars[j]) = true;
            }
        }
        if (strcmp(argv[i], "--help") == 0) {
            std::cerr << "Runs a stress test on inserts, deletes, and finds" << std::endl;
            std::cerr << "Arguments:" << std::endl;
            for (size_t j = 0; j < arg_num; j++) {
                std::cerr << args[j] << ":\t" << arg_help[j] << std::endl;
            }
            for (size_t j = 0; j < flag_num; j++) {
                std::cerr << flags[j] << ":\t" << flag_help[j] << std::endl;
            }
            exit(0);
        }
    }
}
