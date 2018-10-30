// common.h
#ifndef _ENGINE_RACE_COMMON_H_
#define _ENGINE_RACE_COMMON_H_

#include <iostream>
#include <stdio.h>
#include <string>
#include <time.h>
#include <stdarg.h>
#include "config.h"

namespace polar_race {
using std::cout;
void DEBUG(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " DEBUG:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}

void INFO(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " INFO:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}

void ERROR(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " ERROR:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}
}

#endif // _ENGINE_RACE_COMMON_H_