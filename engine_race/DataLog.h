// DBLog.h
#ifndef _ENGINE_RACE_LOG_H_
#define _ENGINE_RACE_LOG_H_

#include "../include/polar_string.h"
#include "../include/engine.h"
#include "MemTable.h"
#include "util.h"
#include <map>
#include <stdio.h>
#include <mutex>
namespace polar_race {

class DataLog {
public:
    static RetCode log(PolarString& key, PolarString& value, int _id);
    static RetCode createLog(int _id);
    static RetCode deleteLog(int _id);
    static RetCode recover(MemTable* table);
private:
    DataLog(){}
    static std::map<int, WritableFile*> log_files;

};

} // namespace poalr_race

#endif // _ENGINE_RACE_LOG_H_