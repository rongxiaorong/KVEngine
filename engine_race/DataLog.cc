// DataLog.cc

#include "DataLog.h"
#include <sys/stat.h>
#include <sstream>
#include <stdio.h>

namespace polar_race {

std::map<int, WritableFile*> DataLog::log_files;

RetCode DataLog::log(const PolarString& key, const PolarString& value, int _id) {
    static std::mutex _log_mtx;
    std::lock_guard<std::mutex> guard(_log_mtx);
    if (log_files.find(_id) == log_files.end()) {
        // new a new log file
        ASSERT(createLog(_id));
    }
    WritableFile* log_file = log_files[_id];
    
    // This will be updated as batch write
    size_t key_size = key.size();
    size_t value_size = value.size();

    ASSERT(log_file->append((char*)&key_size, sizeof(size_t)));
    ASSERT(log_file->append(key.data(), key.size()));
    ASSERT(log_file->append((char*)&value_size, sizeof(size_t))); 
    ASSERT(log_file->append(value.data(), value.size()));
    ASSERT(log_file->flush());

    return RetCode::kSucc;
}

RetCode DataLog::createLog(int _id) {
    if (log_files.find(_id) != log_files.end())
        return RetCode::kNotFound;
    // get log file name
    std::stringstream sstream;
    sstream << __engine_dir << "/" <<LOG_FILE_NAME << _id;
    std::string log_name;
    sstream >> log_name;
    
    // create log file
    WritableFile* new_file = new WritableFile();
    ASSERT(new_file->open(log_name.c_str()));
    log_files[_id] = new_file;
    INFO("Creating DataLog: %s", log_name.c_str());

    // resize the file
    struct stat statbuf;
    stat(log_name.c_str(), &statbuf);
    size_t _fsize = statbuf.st_size;
    size_t valid_size = _fsize - (_fsize % (4096 + 8 + 2 * sizeof(size_t)));
    if (_fsize != valid_size) {
        ERROR("_fsize != valid_size, %ld %ld", _fsize, valid_size);
        new_file->seek(valid_size);
    }
    return RetCode::kSucc;
}

RetCode DataLog::deleteLog(int _id) {
    static std::mutex _delete_mtx;
    std::lock_guard<std::mutex> guard(_delete_mtx);
    // when a SStable is completely flush into the disk
    // so that the logfile is useless.
    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << LOG_FILE_NAME << _id;
    std::string log_name;
    sstream >> log_name;

    if (log_files.find(_id) != log_files.end()) {
        // DEBUG("DataLog::deleteLog(_id=%d) file is still open?", _id);
        log_files[_id]->close();
    }
    if (remove(log_name.c_str())) {
        // error with remove
        ERROR("DataLog::deleteLog(_id=%d) can't remove file?", _id);
        return RetCode::kIOError;
    }
    return RetCode::kSucc;
}

// RetCode DataLog::recover() {
    // two conditions
    // 1. log exists, sstable exists -> delete sstable, write a new one
    // 2. log exists, sstable not exists  -> write into MemTable  
// }

RetCode DataLog::recover(MemTable* table) {

    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << LOG_FILE_NAME << table->id();
    std::string log_name;
    sstream >> log_name;

    SquentialFile file;
    ASSERT(file.open(log_name));
    INFO("Recovering %s", log_name.c_str());
    size_t size;
    char kbuf[1024];
    char vbuf[8096];
    while(true) {
        size_t read_size = 0;
        // read key
        file.read((char*)&size, sizeof(size_t), read_size);
        if (read_size == 0)
            break;
        file.read(kbuf, size, read_size);
        PolarString key(kbuf, size);

        file.read((char*)&size, sizeof(size_t), read_size);
        file.read(vbuf, size, read_size);
        PolarString value(vbuf, size);

        if(table->write(key, value) != RetCode::kSucc)
            ERROR("DataLog::recover(%d) can't write.", table->id());
    }

    // deleteLog(table->id());

    return RetCode::kSucc;
}

} // namespace polar_race
