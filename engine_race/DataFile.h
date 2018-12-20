#ifndef _ENGINE_RACE_DATAFILE_H_
#define _ENGINE_RACE_DATAFILE_H_

#include "util.h"
#include "config.h"
#include <atomic>
namespace polar_race {


extern string engineDir;

class DataFile {
public:
    DataFile(){}
    int id() {return _id;}
    unsigned long size() {return _size;}
    RetCode open(int n);
    RetCode write(const char* key, const char* value, const unsigned long stamp, unsigned long &offset);
    RetCode read(const unsigned long &offset, DataEntry *entry);
    RetCode readValue(const unsigned long &offset, char *value);
    RetCode last(int n, DataEntry *entry);
    RetCode first(int n, DataEntry *entry);
private: 
    char* write_buf = nullptr;
    int _id = -1;
    int _fd = 0;
    unsigned long _size = 0;
    int _count;
    std::mutex _write_lock;
    std::condition_variable _cv;
};
extern DataFile dataFile[DATA_FILE_NUM];

RetCode writeTable(const char* key, const char* value, const unsigned long stamp, unsigned long *offset);
} // namespace polar_race


#endif