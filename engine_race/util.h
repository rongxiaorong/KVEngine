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

using std::cout;
using std::string;
void DEBUG(const char* format, ...);

void INFO(const char* format, ...);

void ERROR(const char* format, ...);



// class WritableFile {
// public:
//     WritableFile(int fd);
//     WritableFile();
//     RetCode open(int fd);
//     RetCode open(const char* file_name);
//     RetCode append(const char* data, const size_t &size);
//     RetCode flush();
//     RetCode close();
//     size_t size();
//     ~WritableFile();
// private:
//     const static size_t MAX_BUFFER_SIZE = 64 * 1024;
//     RetCode _flush();
//     int _fd;
//     size_t _buf_size = 0;
//     size_t _data_size = 0;
//     char _buf[MAX_BUFFER_SIZE + 1];
// };

// class SquentialFile {
// public:
//     SquentialFile(int fd);
//     SquentialFile();
//     ~SquentialFile();
//     RetCode open(const string &file);
//     RetCode open(int fd);
//     RetCode read(char* dst, const size_t &size, size_t &ret_size);
//     RetCode close();
// private:
//     RetCode _read();
//     const static size_t MAX_BUFFER_SIZE = 64 * 1024;
//     int _fd;
//     size_t _data_pos = 0;
//     size_t _buf_pos = 0;
//     size_t _buf_size = 0;
//     char _buf[MAX_BUFFER_SIZE];
// };

// // class MmapFile {
// // public:
// //     MmapFile():_fd(-1):_size(0){}
// //     ~MmapFile() {close();}
// //     RetCode open(const string &file) {
// //         int fd = ::open(file.c_str(), O_RDONLY);
// //         return open(fd);
// //     }
// //     RetCode open(int fd) {
// //         if (fd < 0) {
// //             ERROR("MmapFile::open() open a wrong file.");
// //             return RetCode::kIOError;
// //         }
// //         else {
// //             _fd = fd;
// //             size_t map_size = 100;
// //             _ptr = mmap(NULL, map_size, PROT_READ, MAP_SHARED, _fd, 0);
// //             if (_ptr == nullptr) {
// //                 ERROR("MmapFile::open() mmap error.");
// //                 return RetCode::kIOError;
// //             }
// //             return RetCode::kSucc;
// //         }
// //     }
// //     RetCode read(char* buf, const size_t &begin, const size_t &len) {
// //         if (_ptr == nullptr)
// //             return RetCode::kIOError;
// //         memcpy(buf, _ptr + begin, len);
// //         return RetCode::kSucc;
// //     }
// //     size_t size() {
// //         return _size;
// //     }
// //     RetCode close() {
// //         if (_fd > 0) {
// //             ::close(_fd);
// //             _fd = -1;
// //         }
// //         return RetCode::kSucc;
// //     }
// // private:
// //     char* _ptr;
// //     size_t _size;
// //     int _fd;
// // };

// class RandomAccessFile {
// public:
//     RandomAccessFile();
//     ~RandomAccessFile();
//     RetCode open(const string &file);
//     RetCode open(const int &fd);
//     RetCode read(void* buf, const size_t &begin, const size_t &len);
// private:
//     int _fd;
// };
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
        return open(fd);
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
    };
    size_t size() { return _data_size + _buf_size;}
    ~WritableFile() {
        close();
    }
private:
    const static size_t MAX_BUFFER_SIZE = 64 * 1024;
    RetCode _flush() {
        ssize_t write_size = ::write(_fd, _buf, _buf_size);
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
            return RetCode::kSucc;
        }
    }
    RetCode read(char* dst, const size_t &size, size_t &ret_size) {
        if (_fd < 0) {
            ERROR("SquentialFile::read() open a wrong file.");
            return RetCode::kIOError;
        }
        if (size <= _buf_size - _buf_pos) {
            // have cache
            memcpy(dst, _buf + _buf_pos, size);
            ret_size = size;
            _buf_pos += size;
            return RetCode::kSucc;
        }

        // preload the buffer
        _read();

        size_t temp = size;
        while (temp > 0 && _buf_size > 0) {
            // copy as much as possible
            size_t cp_size = std::min<size_t>(_buf_size - _buf_pos, temp);
            memcpy(dst, _buf + _buf_pos, cp_size);
            _buf_pos += cp_size;
            temp -= cp_size;
            // load again
            _read();
        }
        ret_size = size - temp;
        return RetCode::kSucc;
    }
    RetCode close() {
        if (_fd > 0) {
            ::close(_fd);
            _fd = 1;
            return RetCode::kSucc;
        }
    }
private:
    RetCode _read() {
        if (_buf_size == _buf_pos) { 
            _buf_size = ::read(_fd, _buf, MAX_BUFFER_SIZE);
            _buf_pos = 0;
        }
        return RetCode::kSucc;
    }
    const static size_t MAX_BUFFER_SIZE = 64 * 1024;
    int _fd;
    size_t _data_pos = 0;
    size_t _buf_pos = 0;
    size_t _buf_size = 0;
    char _buf[MAX_BUFFER_SIZE];
};

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
        ssize_t ret = ::pread(_fd, buf, len, begin);
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


int getSSTableNum();
bool haveLog(int id);
bool testSSTable(int _id);

}// namespace polar_race



#endif //_POLAR_RACE_UTIL_H_
