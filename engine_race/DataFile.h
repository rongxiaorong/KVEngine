#ifndef _ENGINE_RACE_DATAFILE_H_
#define _ENGINE_RACE_DATAFILE_H_

#include "util.h"
#include "config.h"

namespace polar_race {

extern DataFile dataFile[DATA_FILE_NUM];

class DataFile {
public:
    int id();
    unsigned long size();
    RetCode open(int n);
    RetCode write(const char* key, const char* value, const unsigned long stamp, unsigned long &offset);
    RetCode read(const unsigned long &offset, DataEntry *entry);
private:
    int _id;
    int _fd;
    unsigned long _size;
};


} // namespace polar_race


#endif