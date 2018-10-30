// util.h

#ifndef _POLAR_RACE_UTIL_H_
#define _POLAR_RACE_UTIL_H_

#include "../../include/engine.h"
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <time.h>
#include <stdarg.h>
#include "config.h"

namespace polar_race {

#define ASSERT(A) {RetCode ret;if((ret = (A))!=RetCode::kSucc) return ret;}

using std::cout;

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
        int fd = ::open(file_name, O_CREAT|O_APPEND|O_WRONLY);
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

};

}// namespace polar_race



#endif //_POLAR_RACE_UTIL_H_
