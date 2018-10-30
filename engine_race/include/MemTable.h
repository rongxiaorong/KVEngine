// MemTable.h
#ifndef _ENGINE_RACE_MEMTABLE_H_
#define _ENGINE_RACE_MEMTABLE_H_

#include "common.h"
#include "config.h"
#include "engine.h"
#include "skiplist/sl_map.h"
#include <atomic>
#include <mutex>
#include <list>
#include <string>

namespace polar_race {
using std::string;

int TABLE_COUNT;

class MemTable {
public:
    static MemTable* getMemtable();
    static MemTable* getImmut();

    MemTable(){
        _id = TABLE_COUNT;
        TABLE_COUNT++;
    }

    // set a mutex for write a new entry 
    // update entries can be free
    RetCode write(const PolarString& key, const PolarString& value);
    
    bool contains(const PolarString& key);

    RetCode read(const PolarString& key, PolarString& value);
    
    int id(){return _id;}

private:
    static std::mutex table_mtx;
    static std::mutex immut_mtx;
    
    static MemTable* _mutable;
    static MemTable* immut;
    
    int _id;

    sl_map<string, string*> index;

    std::atomic_long size;

    std::mutex _write_mtx;
    RetCode _write(const PolarString& key, const PolarString& value);
    
    RetCode _update(const PolarString& key, const PolarString& value);
    
    bool _immut = false;
    void setImmutable();
};
} // namespace polar_race
#endif //_ENGINE_RACE_MEMTABLE_H_