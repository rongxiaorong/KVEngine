// util.h

#ifndef _POLAR_RACE_UTIL_H_
#define _POLAR_RACE_UTIL_H_

#include "../include/engine.h"
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <time.h>
#include <stdarg.h>
#include <sstream>
#include <sys/stat.h>
#include <mutex>
#include <queue>
#include <condition_variable>
#include "config.h"
namespace polar_race {

#define ASSERT(A)                     \
do {                                  \
    RetCode ret;                      \
    if((ret = (A))!=RetCode::kSucc) { \
        ERROR(#A);                    \
        return ret;                   \
    }                                 \
}while(0)                             \

struct IndexEntry {
    char k[8];
    int n;
    int p;
};

using std::cout;
using std::string;
void DEBUG(const char* format, ...);

void INFO(const char* format, ...);

void ERROR(const char* format, ...);

void CHECKIN(const char* format);
void Watcher();
class WritableFile {
public:
    WritableFile(int fd):_fd(fd){}
    WritableFile():_fd(-1){}
    RetCode open(int fd) {
        if (fd < 0) {
            ERROR("WritableFile::open() open a wrong file.");
            return RetCode::kIOError;
        }
        else {
            _fd = fd;
            return RetCode::kSucc;
        }
    }
    RetCode open(const char* file_name) {
        int fd = ::open(file_name, O_WRONLY |O_CREAT | O_APPEND , 0777);
        // lseek(fd, )
        return open(fd);
    }
    RetCode seek(size_t pos) {
        size_t res;
        if ((res = lseek(_fd, pos, SEEK_SET)) != pos) {
            ERROR("lseek error : res %ld pos %ld", res, pos);
            return RetCode::kIOError;
        }
        return RetCode::kSucc;
    }
    RetCode append(const char* data, const size_t &size) {
        if (_fd < 0) {
            ERROR("WritableFile::append() open a wrong file.");
            return RetCode::kIOError;
        }
        // try to write into _buf
        if (size < MAX_BUFFER_SIZE - _buf_size) {
            memcpy(_buf + _buf_size, data, size);
            _buf_size += size;
        }
        else {
            size_t temp = size;
            while (temp > 0) {
                size_t cpy_size = std::min<size_t>(temp, MAX_BUFFER_SIZE - _buf_size);
                memcpy(_buf + _buf_size, data + size - temp, cpy_size);
                _buf_size += cpy_size;
                temp -= cpy_size;
                if (temp > 0){
                    RetCode flush_ret = _flush();
                    if (flush_ret != RetCode::kSucc)
                        return flush_ret;
                }
            }
        }
        return RetCode::kSucc;
    }
    RetCode flush() {
        if (_fd < 0) {
            ERROR("WritableFile::flush() open a wrong file.");
            return RetCode::kIOError;
        }
        return _flush();
    };
    RetCode close() {
        if(_fd > 0) {
            _flush();
            ::close(_fd);
            _fd = -1;
        }
        return kSucc;
    };
    size_t size() { return _data_size + _buf_size;}
    ~WritableFile() {
        close();
    }
private:
    const static size_t MAX_BUFFER_SIZE = 64 * 1024;
    RetCode _flush() {
        size_t write_size = ::write(_fd, _buf, _buf_size);
        _data_size += write_size;
        if (write_size != _buf_size) {
            ERROR("WritableFile::_flush() try to write %d size, but write %d size.", (int)_buf_size, (int)write_size);
            _buf_size = 0;
            return RetCode::kIOError;
        }
        _buf_size = 0;
        return RetCode::kSucc;
    }
    int _fd;
    size_t _buf_size = 0;
    size_t _data_size = 0;
    char _buf[MAX_BUFFER_SIZE + 1];
};

class RandomAccessFile {
public:
    RandomAccessFile():_fd(-1){}
    ~RandomAccessFile() {
        this->close();
    }
    RetCode open(const string &file) {
        int fd = ::open(file.c_str(), O_RDONLY);
        return open(fd);
    }
    RetCode open(const int &fd) {
        if (fd < 0) {
            ERROR("RandomAccessFile::open() fail");
            return RetCode::kIOError;
        }
        _fd = fd;
        return RetCode::kSucc;
    }
    RetCode read(void* buf, const size_t &begin, const size_t &len) {
        if (_fd < 0)
            return RetCode::kNotFound;
        size_t ret = ::pread(_fd, buf, len, begin);
        if (len != ret)
            INFO("RandomAccessFile::read() try to read %d bytes, but get %d bytes", (int)len, (int)ret);
        if (ret <= 0)
            return RetCode::kIOError;
        return RetCode::kSucc;
    }
    RetCode close() {
        if (_fd > 0)
            ::close(_fd);
        return RetCode::kSucc;
    }
private:
    int _fd;
};

class SquentialFile {
public:
    SquentialFile(int fd):_fd(fd){}
    SquentialFile():_fd(-1){}
    ~SquentialFile() {
        close();
    }
    RetCode open(const string &file) {
        int fd = ::open(file.c_str(), O_RDONLY);
        return open(fd);       
    }
    RetCode open(int fd) {
        if (fd < 0) {
            ERROR("SquentialFile::open() open a wrong file.");
            return RetCode::kIOError;
        }
        else {
            _fd = fd;
            _raf.open(fd);
            _data_size = lseek(_fd, 0, SEEK_END);
            _pos = lseek(_fd, 0, SEEK_SET);
            return RetCode::kSucc;
        }
    }
    RetCode read(char* dst, const size_t &size, size_t &ret_size) {
        if (_fd < 0) {
            ERROR("SquentialFile::read() open a wrong file.");
            return RetCode::kIOError;
        }
        if ( _data_size >= size + _pos) {
            RetCode ret = _raf.read(dst, _pos, size);
            _pos += size;
            ret_size = size;
            return ret;
        }
        else {
            RetCode ret = _raf.read(dst, _pos, ret_size);
            _pos = _data_size;
            ret_size = _data_size - _pos;
            return ret;
        }
    }
    RetCode close() {
        return _raf.close();
    }
private:
    int _fd = -1;
    size_t _data_size = 0;
    size_t _pos = 0;
    RandomAccessFile _raf;
};

class MemTable;
class MemoryManager {
public:
    MemoryManager():_head(nullptr){};
    ~MemoryManager(){if(_head) ::free(_head);};
    void init(int n);
    void* allocate();
    void release() {
        if (_head)
            ::free(_head);
        _head = nullptr;
    }
    void free(const void * target);
    void free(const MemTable* table, bool flag);
private:
    const static size_t block_size = 4096;
    char* _head;
    int _n;
    std::queue<int> _free;
    std::mutex mtx;
    std::condition_variable cv;
};


int getSSTableNum();
bool haveLog(int id);
bool testSSTable(int _id);

}// namespace polar_race



#endif //_POLAR_RACE_UTIL_H_
