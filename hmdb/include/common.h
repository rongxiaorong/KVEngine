// common.h
#ifndef _HMDB_COMMON_H_
#define _HMDB_COMMON_H_

#include <iostream>
#include <stdio.h>
#include <string>
#include <time.h>

#include "config.h"

namespace polar_race {
using std::cout;
void DEBUG(char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " DEBUG:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}

void INFO(char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " INFO:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}

void ERROR(char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " ERROR:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}
}

#endif // _HMDB_COMMON_H_