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
#include <atomic>
#include "config.h"
#include "util.h"

namespace polar_race {

using std::cout;
using std::string;

std::atomic_int check(0);
string check_info;
void CHECKIN(const char* info) {
    static std::mutex mtx;
    check.fetch_add(1);
    mtx.lock();
    check_info.assign(info);
    mtx.unlock();
}

void Watcher() {
    int record = check.fetch_add(0);
    int new_record;
    do {
        record = check.fetch_add(0);
        sleep(30);
        new_record = check.fetch_add(0);
    } while(new_record != record);
    INFO("Fucking locked :%s", check_info.c_str());
    exit(-1);    
}

void DEBUG(const char* format, ...) {
    //static FILE* fd = fopen("/home/francis/Git/tmp_debug_file" , "a+");
    //
    //va_list args;
    //va_start(args, format);
    //fprintf(fd,"%ld DEBUG:",time(NULL));
    //vfprintf(fd, format, args);
    // fprintf(fd,"\n");
    // fflush(fd);
    //va_end(args);
}

void INFO(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " INFO:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
    CHECKIN(format);
}

void ERROR(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " ERROR:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}

string DataFileName(int i) {
    std::stringstream sstream;
    sstream << DATA_FILE_NAME << i;
    string ret;
    sstream >> ret;
    return ret;
}


bool fileExist(const char* file_name) {
    return access(file_name, F_OK) != -1;
}

bool fileExist(const char* dir, const char* file_name) {
    string _dir(dir), _file(file_name);
    string fullname = _dir + "/" + _file;
    return fileExist(fullname.c_str());
}

size_t fileSize(const char* file_name) {
    // get file size
    struct stat statbuf;
    stat(file_name, &statbuf);
    return statbuf.st_size;   
}
}// namespace polar_race


