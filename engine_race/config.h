// config.h
#ifndef _ENGINE_RACE_CONFIG_H_
#define _ENGINE_RACE_CONFIG_H_

#define MAX_ENTRY 67108864

// max memory in use (byte)
// 1.5GB
#define MAX_MEM 1610612736

// block in a memtable
// 64MB
#define MEM_BLOCK_SIZE 67108864

// memtable max size
// 500MB
#define MEMTABLE_MAX_SIZE 512000000


#define LOG_FILE_NAME "DBLOG"

#define SSTABLE_FILE_NAME "SSTABLE"

#define MAGIC_STRING "XIAOCONG"

#define MAX_FILTER_CACHE 5

#define MAX_BLOCK_CACHE 5

#include <string>

namespace polar_race {
std::string ENGINE_DIR;
} // namespace polar_race
#endif // _ENGINE_RACE_CONFIG_H_