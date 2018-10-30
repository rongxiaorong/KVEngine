// DataLog.cc

#include "DataLog.h"
#include <sstream>
#include <stdio.h>
namespace polar_race {

std::map<int, WritableFile*> DataLog::log_files;

RetCode DataLog::log(PolarString& key, PolarString& value, int _id) {
    static std::mutex _log_mtx;
    std::lock_guard<std::mutex> guard(_log_mtx);
    if (log_files.find(_id) == log_files.end()) {
        // new a new log file
        ASSERT(createLog(_id));
    }
    WritableFile* log_file = log_files[_id];
    
    // This will be updated as batch write
    int key_size = key.size();
    int value_size = value.size();

    ASSERT(log_file->append((char*)&key_size, sizeof(int)));
    ASSERT(log_file->append(key.data(), key.size()));
    ASSERT(ret = log_file->append((char*)&value_size, sizeof(int))); 
    ASSERT(ret = log_file->append(value.data(), value.size()))
    ASSERT(ret = log_file->flush());

    return RetCode::kSucc;
}

RetCode DataLog::createLog(int _id) {
    if (log_files.find(_id) != log_files.end())
        return RetCode::kNotFound;
    // get log file name
    std::stringstream sstream;
    sstream << LOG_FILE_NAME << "_" << _id;
    std::string log_name;
    sstream >> log_name;
    
    // create log file
    WritableFile* new_file = new WritableFile();
    ASSERT(new_file->open(log_name.c_str()));
    log_files[_id] = new_file;

    return RetCode::kSucc;
}

RetCode DataLog::deleteLog(int _id) {
    static std::mutex _delete_mtx;
    std::lock_guard<std::mutex> guard(_delete_mtx);
    // when a SStable is completely flush into the disk
    // so that the logfile is useless.
    std::stringstream sstream;
    sstream << LOG_FILE_NAME << "_" << _id;
    std::string log_name;
    sstream >> log_name;

    if (log_files.find(_id) != log_files.end()) {
        DEBUG("DataLog::deleteLog(_id=%d) file is still open?", _id);
        log_files[_id]->close();
    }
    if (remove(log_name.c_str())) {
        // error with remove
        ERROR("DataLog::deleteLog(_id=%d) can't remove file?", _id);
        return RetCode::kIOError;
    }
    return RetCode::kSucc;
}

RetCode DataLog::recover() {
    // two conditions
    // 1. log exists, sstable exists -> delete sstable, write a new one
    // 2. log exists, sstable not exists  -> write into MemTable
    
    
}



} // namespace polar_race