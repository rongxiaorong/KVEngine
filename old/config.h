// config.h
#ifndef _ENGINE_RACE_CONFIG_H_
#define _ENGINE_RACE_CONFIG_H_

#define MAX_ENTRY 67108864

// max memory in use (byte)
// 1.5GB
#define MAX_MEM 1610612736

// block in a memtable
// 64MB
#define MEM_BLOCK_SIZE 45 * 1024

// memtable max size
// 500MB
#define MEMTABLE_MAX_SIZE 94200000//512000000

#define HASHTABLE_SIZE 64000000

#define LOG_FILE_NAME "DBLOG"

#define SSTABLE_FILE_NAME "SSTABLE"

#define INDEX_FILE_NAME "INDEX"

#define MAGIC_STRING "XIAOCONG"

#define MAX_FILTER_CACHE 5

#define MAX_INDEX_CACHE 0

#define FILTER_SIZE 4*1024*1024*8
#include <string>

namespace polar_race {
// std::string ENGINE_DIR;
extern std::string __engine_dir;
struct MetaData {
    int SStable_num;
    // int 
};
} // namespace polar_race
#endif // _ENGINE_RACE_CONFIG_H_
