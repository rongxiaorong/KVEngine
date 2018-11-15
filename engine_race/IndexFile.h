#ifndef _ENGINE_RACE_INDEXFILE_H_
#define _ENGINE_RACE_INDEXFILE_H_

#include "config.h"
#include "util.h"
#include <map>
#include <mutex>
namespace polar_race {

extern MemIndex memIndex;

class IndexFile {
public:
    RetCode open(bool is_tmp);
    IndexEntry* next();
};

class MemIndex {
public:
    RetCode insert(const char* key, const int &num, const unsigned long offset);
private:
    std::map<string, IndexEntry> * _index;
    std::mutex _write_mtx;
};

} // namespace polar_race



#endif