// util.cc
#include "../include/engine.h"
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <time.h>
#include <stdarg.h>
#include <sstream>
#include <sys/stat.h>
#include "config.h"
#include "util.h"

namespace polar_race {

using std::cout;
using std::string;

void DEBUG(const char* format, ...) {
    static FILE* fd = fopen("/home/francis/Git/tmp_debug_file" , "a+");
    
    va_list args;
    va_start(args, format);
    fprintf(fd,"%ld DEBUG:",time(NULL));
    vfprintf(fd, format, args);
    fprintf(fd,"\n");
    fflush(fd);
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

int getSSTableNum() {
    for (int i = 0;; i++) {
        std::stringstream sstream;
        sstream <<  __engine_dir << "/" << SSTABLE_FILE_NAME << i;
        std::string table_name; 
        sstream >> table_name;
        if (access(table_name.c_str(), F_OK) == -1)
            return i;
    }
}
bool haveLog(int id) {
  
    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << LOG_FILE_NAME << id;
    std::string table_name; 
    sstream >> table_name;
    if (access(table_name.c_str(), F_OK) != -1) {
        INFO("Find Log: %s", table_name.c_str());
        return true;
    }
    else {
        INFO("Can't find log: %s", table_name.c_str());
        return false;
    }
}

bool testSSTable(int _id) {
    if (_id < 0) return true;
    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << LOG_FILE_NAME << _id;
    std::string table_name; 
    sstream >> table_name;
    // get file size
    struct stat statbuf;
    stat(table_name.c_str(), &statbuf);
    size_t _fsize = statbuf.st_size;

    // 
    RandomAccessFile file;
    if (file.open(table_name) != RetCode::kSucc)
        return false;
    char buf[32];
    int magic_len = strlen(MAGIC_STRING);
    if (file.read(buf, _fsize - magic_len, magic_len) != RetCode::kSucc)
        return false;
    if (PolarString(MAGIC_STRING, magic_len).compare(PolarString(buf, magic_len)) != 0)
        return false;
    return true;


}

}// namespace polar_race


