// util.cc
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
#include "config.h"
#include "util.h"

namespace polar_race {

using std::cout;
using std::string;

void DEBUG(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " DEBUG:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}

void INFO(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " INFO:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}

void ERROR(const char* format, ...) {
    va_list args;
    va_start(args, format);
    cout << time(NULL) << " ERROR:";
    vprintf(format, args);
    cout << "\n";
    va_end(args);
}



// class WritableFile {
// public:
//     WritableFile(int fd):_fd(fd){}
//     WritableFile():_fd(-1){}
//     RetCode open(int fd) {
//         if (fd < 0) {
//             ERROR("WritableFile::open() open a wrong file.");
//             return RetCode::kIOError;
//         }
//         else {
//             _fd = fd;
//             return RetCode::kSucc;
//         }
//     }
//     RetCode open(const char* file_name) {
//         int fd = ::open(file_name, O_CREAT | O_APPEND, S_IRWXU | S_IXGRP | S_IROTH | S_IXOTH);
//         return open(fd);
//     }
//     RetCode append(const char* data, const size_t &size) {
//         if (_fd < 0) {
//             ERROR("WritableFile::append() open a wrong file.");
//             return RetCode::kIOError;
//         }
//         // try to write into _buf
//         if (size < MAX_BUFFER_SIZE - _buf_size) {
//             memcpy(_buf + _buf_size, data, size);
//             _buf_size += size;
//         }
//         else {
//             size_t temp = size;
//             while (temp > 0) {
//                 size_t cpy_size = std::min<size_t>(temp, MAX_BUFFER_SIZE - _buf_size);
//                 memcpy(_buf + _buf_size, data + size - temp, cpy_size);
//                 _buf_size += cpy_size;
//                 temp -= cpy_size;
//                 if (temp > 0){
//                     RetCode flush_ret = _flush();
//                     if (flush_ret != RetCode::kSucc)
//                         return flush_ret;
//                 }
//             }
//         }
//         return RetCode::kSucc;
//     }
//     RetCode flush() {
//         if (_fd < 0) {
//             ERROR("WritableFile::flush() open a wrong file.");
//             return RetCode::kIOError;
//         }
//         return _flush();
//     };
//     RetCode close() {
//         if(_fd > 0) {
//             _flush();
//             ::close(_fd);
//             _fd = -1;
//         }
//     };
//     size_t size() { return _data_size + _buf_size;}
//     ~WritableFile() {
//         close();
//     }
// private:
//     const static size_t MAX_BUFFER_SIZE = 64 * 1024;
//     RetCode _flush() {
//         ssize_t write_size = ::write(_fd, _buf, _buf_size);
//         _data_size += write_size;
//         if (write_size != _buf_size) {
//             ERROR("WritableFile::_flush() try to write %d size, but write %d size.", (int)_buf_size, (int)write_size);
//             _buf_size = 0;
//             return RetCode::kIOError;
//         }
//         _buf_size = 0;
//         return RetCode::kSucc;
//     }
//     int _fd;
//     size_t _buf_size = 0;
//     size_t _data_size = 0;
//     char _buf[MAX_BUFFER_SIZE + 1];
// };

// class SquentialFile {
// public:
//     SquentialFile(int fd):_fd(fd){}
//     SquentialFile():_fd(-1){}
//     ~SquentialFile() {
//         close();
//     }
//     RetCode open(const string &file) {
//         int fd = ::open(file.c_str(), O_RDONLY);
//         return open(fd);
//     }
//     RetCode open(int fd) {
//         if (fd < 0) {
//             ERROR("SquentialFile::open() open a wrong file.");
//             return RetCode::kIOError;
//         }
//         else {
//             _fd = fd;
//             return RetCode::kSucc;
//         }
//     }
//     RetCode read(char* dst, const size_t &size, size_t &ret_size) {
//         if (_fd < 0) {
//             ERROR("SquentialFile::read() open a wrong file.");
//             return RetCode::kIOError;
//         }
//         if (size <= _buf_size - _buf_pos) {
//             // have cache
//             memcpy(dst, _buf + _buf_pos, size);
//             ret_size = size;
//             _buf_pos += size;
//             return RetCode::kSucc;
//         }

//         // preload the buffer
//         _read();

//         size_t temp = size;
//         while (temp > 0 && _buf_size > 0) {
//             // copy as much as possible
//             size_t cp_size = std::min<size_t>(_buf_size - _buf_pos, temp);
//             memcpy(dst, _buf + _buf_pos, cp_size);
//             _buf_pos += cp_size;
//             temp -= cp_size;
//             // load again
//             _read();
//         }
//         ret_size = size - temp;
//         return RetCode::kSucc;
//     }
//     RetCode close() {
//         if (_fd > 0) {
//             ::close(_fd);
//             _fd = 1;
//             return RetCode::kSucc;
//         }
//     }
// private:
//     RetCode _read() {
//         if (_buf_size == _buf_pos) { 
//             _buf_size = ::read(_fd, _buf, MAX_BUFFER_SIZE);
//             _buf_pos = 0;
//         }
//         return RetCode::kSucc;
//     }
//     const static size_t MAX_BUFFER_SIZE = 64 * 1024;
//     int _fd;
//     size_t _data_pos = 0;
//     size_t _buf_pos = 0;
//     size_t _buf_size = 0;
//     char _buf[MAX_BUFFER_SIZE];
// };

// class MmapFile {
// public:
//     MmapFile():_fd(-1):_size(0){}
//     ~MmapFile() {close();}
//     RetCode open(const string &file) {
//         int fd = ::open(file.c_str(), O_RDONLY);
//         return open(fd);
//     }
//     RetCode open(int fd) {
//         if (fd < 0) {
//             ERROR("MmapFile::open() open a wrong file.");
//             return RetCode::kIOError;
//         }
//         else {
//             _fd = fd;
//             size_t map_size = 100;
//             _ptr = mmap(NULL, map_size, PROT_READ, MAP_SHARED, _fd, 0);
//             if (_ptr == nullptr) {
//                 ERROR("MmapFile::open() mmap error.");
//                 return RetCode::kIOError;
//             }
//             return RetCode::kSucc;
//         }
//     }
//     RetCode read(char* buf, const size_t &begin, const size_t &len) {
//         if (_ptr == nullptr)
//             return RetCode::kIOError;
//         memcpy(buf, _ptr + begin, len);
//         return RetCode::kSucc;
//     }
//     size_t size() {
//         return _size;
//     }
//     RetCode close() {
//         if (_fd > 0) {
//             ::close(_fd);
//             _fd = -1;
//         }
//         return RetCode::kSucc;
//     }
// private:
//     char* _ptr;
//     size_t _size;
//     int _fd;
// };

// class RandomAccessFile {
// public:
//     RandomAccessFile():_fd(-1){}
//     ~RandomAccessFile() {
//         this->close();
//     }
//     RetCode open(const string &file) {
//         int fd = ::open(file.c_str(), O_RDONLY);
//         return open(fd);
//     }
//     RetCode open(const int &fd) {
//         if (fd < 0) {
//             ERROR("RandomAccessFile::open() fail");
//             return RetCode::kIOError;
//         }
//         _fd = fd;
//         return RetCode::kSucc;
//     }
//     RetCode read(void* buf, const size_t &begin, const size_t &len) {
//         if (_fd < 0)
//             return RetCode::kNotFound;
//         ssize_t ret = ::pread(_fd, buf, len, begin);
//         if (len != ret)
//             INFO("RandomAccessFile::read() try to read %d bytes, but get %d bytes", (int)len, (int)ret);
//         if (ret <= 0)
//             return RetCode::kIOError;
//         return RetCode::kSucc;
//     }
//     RetCode close() {
//         if (_fd > 0)
//             ::close(_fd);
//         return RetCode::kSucc;
//     }
// private:
//     int _fd;
// };

int getSSTableNum() {
    for (int i = 0;; i++) {
        std::stringstream sstream;
        sstream <<  __engine_dir << "/" << SSTABLE_FILE_NAME << i;
        std::string table_name; 
        sstream >> table_name;
        if (access(table_name.c_str(), F_OK) == -1)
            return i;
    }
}
bool haveLog(int id) {
  
    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << LOG_FILE_NAME << id;
    std::string table_name; 
    sstream >> table_name;
    if (access(table_name.c_str(), F_OK) != -1)
        return true;
    else
        return false;
}

bool testSSTable(int _id) {
    if (_id < 0) return true;
    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << LOG_FILE_NAME << _id;
    std::string table_name; 
    sstream >> table_name;
    // get file size
    struct stat statbuf;
    stat(table_name.c_str(), &statbuf);
    size_t _fsize = statbuf.st_size;

    // 
    RandomAccessFile file;
    if (file.open(table_name) != RetCode::kSucc)
        return false;
    char buf[32];
    int magic_len = strlen(MAGIC_STRING);
    if (file.read(buf, _fsize - magic_len, magic_len) != RetCode::kSucc)
        return false;
    if (PolarString(MAGIC_STRING, magic_len).compare(PolarString(buf, magic_len)) != 0)
        return false;
    return true;


}

}// namespace polar_race

