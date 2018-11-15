// Copyright [2018] Alibaba Cloud All rights reserved
#include <dirent.h>
#include "engine_race.h"
#include "util.h"
#include "DataLog.h"
#include "MemTable.h"
#include "TableIO.h"
#include "config.h"
#include "IndexIO.h"
namespace polar_race {

// std::string ENGINE_DIR;

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);

  *eptr = engine_race;
  return kSucc;
}

int TABLE_COUNT = 0;
std::string __engine_dir;
bool _test_flag = false;
bool _index_flag = false;
MemoryManager memManager;
IndexReader *indexReader = nullptr;

EngineRace::EngineRace(const std::string& dir) {
  // initilize the engine
  std::thread watcher(Watcher);
  watcher.detach();
  __engine_dir.assign(dir);
  INFO("Initializing...");    
  INFO("_test_flag = %s", _test_flag?"true":"false");
  // if(true) {
    DIR* _dir = opendir(dir.c_str());
    if (_dir == NULL) {
      if (mkdir(dir.c_str(), 0777) == 0)
        INFO("Create dir %s.", dir.c_str());
      else _dir = opendir(".");
      _index_flag = true;
      std::thread* index_writer = new std::thread(IndexMerger);
      thread_list.push_back(index_writer);
    }
    ::closedir(_dir);

    int sstable_num = getSSTableNum();
    // int i = sstable_num - 1;
    INFO("Find tables:%d",sstable_num);
    for (int i = 0; i < sstable_num; i++) {
        TableReader* table = new TableReader();
        if (table->open(i) != kSucc)
          ERROR("Can't open table%d when initializing.", i);
        else {
          if (table->testMagic() == kSucc)
            // can be used
            SSTableMap[i] = table;  
          else {
            // recover
            delete table;
            MemTable* memtable = new MemTable();
            memtable->setID(i);
            memtable->unset_auto_write();
            if (DataLog::recover(memtable) != RetCode::kSucc) 
              ERROR("Recover Error with table%d", i);
            writeImmutTable(memtable);
            table = new TableReader();
            table->open(i);
            SSTableMap[i] = table;
          }
        } 
    }
    if (sstable_num > 200) {
      // set IndexReader
      indexReader = new IndexReader();
      indexReader->open();
    }

    TABLE_COUNT = sstable_num;
    if(!_test_flag) {
    MemTable* table = new MemTable();
    if (haveLog(sstable_num)) {
      // table->unset_auto_write();
      INFO("Recover Memtable from DBLOG%d", sstable_num);
      if (DataLog::recover(table) != RetCode::kSucc) 
        ERROR("Recover last Memtable");
      INFO("Memtable size:%ld", table->get_size());
    }
  } else {
    if (MemTable::getMemtable() == NULL) {
      //  memManager.init(MEM_BLOCK_SIZE / 1024);
       MemTable* table_ = new MemTable();
    }
    indexReader = new IndexReader();
    indexReader->open();
  }
  CHECKIN("Finish init");
}

// 2. Close engine
EngineRace::~EngineRace() {
  INFO("Exiting...");
  MemTable* table = MemTable::getMemtable();
  if (table->get_size() > 0) {
    writeImmutTable(table);
  }
  INFO("wait %d thread", thread_list.size());
  int i = 0;
  end_signal = true;
  index_merger_cv.notify_all();
  for(auto iter : thread_list) {
    auto id = i++;
    INFO("thread exiting %d", id);
    iter->join();
    INFO("thread exited! %d", id);
  }
  // for(int i = 0; i < SSTableMap.s) {
    // if (iter)
      // delete iter;
  // }
  _test_flag = true;
  INFO("886");
}

// 3. Write a key-value pair into engine
std::mutex engineWriteMtx;
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  // static std::mutex mtx;
  // std::lock_guard<std::mutex> guard(engineWriteMtx);
  CHECKIN("Write");
  // std::mutex mtx_for_write;
  // std::unique_lock<std::mutex> ulock(mtx_for_write);
  // while(immutTableList.count.fetch_add(0) > 1)
  //   immutTableList._write_for_all.wait(ulock);
  MemTable* table = MemTable::getMemtable();
  ASSERT(table->write(key, value));
  ASSERT(DataLog::log(key, value, table->id()));

  //INFO("Write key:%d size:%d",key.size(),value.size());
  return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  static std::mutex mtx1;
  static std::mutex mtx2;
  static std::atomic_long read_count(0);
  CHECKIN("Read");
  // if (TABLE_COUNT > 200) {
  //   INFO("What are you doing?");
  //   return kNotFound;
  // }
  // if (read_count.fetch_add(1) >= 1000000) {
  //   INFO("8866");
  //   return kNotFound;
  // }
  // mtx1.lock();
  MemTable* table = MemTable::getMemtable();
  if(table != nullptr)
    if (table->read(key, value) == RetCode::kSucc) {
      //INFO("Find key in memtable %d size:%d", table->id(), value->size());
      return kSucc;
    }
  // mtx1.unlock();
  
  if (indexReader != nullptr) {
    IndexEntry* entry = indexReader->find(key);
    if (entry != nullptr) {
      RetCode ret = SSTableMap[entry->n]->readValue(entry->p, value);
      // INFO("HHH");
      return ret;
    } else {
      // INFO("Can't find in indexReader");
      return kNotFound;
    }
  }
  // mtx2.lock();
  std::lock_guard<std::mutex> guard(mtx2);
  string _key = key.ToString();
  for (int i = table->id() - 1; i >= 0; i--) {
    // MemTable* immut_table = immutTableList.get(i);
    // if (immut_table != nullptr) {
    //   if (immut_table->read(key, value) == RetCode::kSucc) {
    //     //INFO("Find key in ImmutTable %d", i);
    //     return kSucc;
    //   } else
    //     continue;
    // }
    TableReader* reader = SSTableMap[i];
    if (reader != nullptr) 
      if (reader->read(_key, value) == RetCode::kSucc) {
        //INFO("Find key in SSTable %d", i);        
        return kSucc;
      }
  }
  // mtx2.unlock();
  // INFO("Not Fin")
  return kNotFound;
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  return kSucc;
}

}  // namespace polar_race
