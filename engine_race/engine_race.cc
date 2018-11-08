// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "util.h"
#include "DataLog.h"
#include "MemTable.h"
#include "TableIO.h"
#include "config.h"
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
EngineRace::EngineRace(const std::string& dir) {
  // initilize the engine
  __engine_dir.assign(dir);
  INFO("Initializing...");
  int sstable_num = getSSTableNum();
  // int i = sstable_num - 1;
  DEBUG("Find tables:%d",sstable_num);
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
          if (writeImmutTable(memtable) != RetCode::kSucc) 
            ERROR("Recover ImmutTable error");
          table = new TableReader();
          table->open(i);
          SSTableMap[i] = table;
        }
      } 
  }
  // while (!testSSTable(i)) {
  //   // invalid table
  //   INFO("Recovering table%d", i);
  //   MemTable* table = new MemTable();
  //   table->setID(i);
  //   table->unset_auto_write();
  //   if (DataLog::recover(table) != RetCode::kSucc) {
  //     ERROR("Recover Error with table%d", i);
  //   }
  //   if (writeImmutTable(table) != RetCode::kSucc) {
  //     ERROR("Recover ImmutTable error");
  //   }
  //   i--;
  // }

  TABLE_COUNT = sstable_num;
  MemTable* table = new MemTable();
  if (haveLog(sstable_num)) {
    if (DataLog::recover(table) != RetCode::kSucc) 
      ERROR("Recover last Memtable");
  }
}

// 2. Close engine
EngineRace::~EngineRace() {
  MemTable* table = MemTable::getMemtable();
  if (writeImmutTable(table) != RetCode::kSucc) {
      ERROR("Close Engine error");
  }
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
  MemTable* table = MemTable::getMemtable();
  ASSERT(DataLog::log(key, value, table->id()));
  ASSERT(table->write(key, value));
  return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
  MemTable* table = MemTable::getMemtable();
  if(table != nullptr)
    if (table->read(key, value) == RetCode::kSucc)
      return kSucc;
  table = MemTable::getImmut();
  if(table != nullptr)
    if (table->read(key, value) == RetCode::kSucc)
      return kSucc;
  string _key = key.ToString();
  for (int i = TABLE_COUNT; i>=0; i--) { 
    TableReader* reader = SSTableMap[i];
    if (reader != nullptr) 
      if (reader->read(_key, value) == RetCode::kSucc)
        return kSucc;
  }
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
