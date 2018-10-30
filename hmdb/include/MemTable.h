// MemTable.h
#ifndef _HMDB_MEMTABLE_H_
#define _HMDB_MEMTABLE_H_

#include "common.h"
#include "config.h"
#include "Allocator.h"
#include "engine.h"
namespace polar_race {
class MemTable {
public:
    MemTable() {
        alloc = new MemAllocator(MEM_BLOCK_SIZE);
    }
    // set a mutex for write a new entry 
    // update entries can be free
    RetCode write(const PolarString& key, const PolarString& value);
    
    bool has(const PolarString& key);

    RetCode read(const PolarString& key, PolarString& value);
    
private:
    MemAllocator* alloc;
    bool _mutable = true;
    
};
} // namespace polar_race
#endif //_HMDB_MEMTABLE_H_