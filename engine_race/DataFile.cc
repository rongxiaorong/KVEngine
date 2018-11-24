#include "DataFile.h"
#include "config.h"
#include "util.h"
#include "IndexFile.h"
// #include <errno.h> 
namespace polar_race {
RetCode DataFile::open(int n) {
    _id = n;
    string name = engineDir + "/" + DataFileName(n);
    int fd = ::open(name.c_str(), O_RDWR | O_CREAT | O_APPEND, 0777);
    if (fd <= 0)
        return kNotFound;
    _fd = fd;
    if (fileExist(name.c_str()))
        _size = fileSize(name.c_str());
    if (_size % sizeof(DataEntry) != 0) {
        lseek(_fd, -(_size % sizeof(DataEntry)), SEEK_END);
        _size -= _size % sizeof(DataEntry);         
    }
    return kSucc;
}

RetCode DataFile::write(const char* key, const char* value, const unsigned long stamp, unsigned long &offset) {
    std::unique_lock<std::mutex> guard(_write_lock);
    while(_size != (stamp / DATA_FILE_NUM) * sizeof(DataEntry))
        _cv.wait(guard);

    offset = _size;
    MemIndexInsert(key, this->_id, offset, stamp);
    DataEntry entry(key, value, stamp);
    size_t ret_size = ::write(_fd, &entry, sizeof(entry));
    _size += ret_size;
    
    _cv.notify_all();
    if (ret_size == sizeof(entry))
        return kSucc;
    else {
        // extern int errno;
        ERROR("Write Error with size:%ld stamp:%ld errorn:%d", ret_size, stamp, 0);
        return kIOError;
    }
}

RetCode DataFile::read(const unsigned long &offset, DataEntry *entry) {
    size_t ret_size = pread(_fd, entry, sizeof(DataEntry), offset);
    if (ret_size == sizeof(DataEntry))
        return kSucc;
    else
        return kIOError;
}

RetCode DataFile::readValue(const unsigned long &offset, char* value) {
    size_t ret_size = pread(_fd, value, 4096, offset + 16);
    if (ret_size == 4096)
        return kSucc;
    else
        return kIOError;
} 
RetCode DataFile::last(int n, DataEntry *entry) {
    unsigned long offset = _size - n * sizeof(DataEntry);
    if (_size < n * sizeof(DataEntry))
        return kNotFound;
    return read(offset, entry);
}
RetCode DataFile::first(int n, DataEntry *entry) {
    unsigned long offset = n * sizeof(DataEntry);
    if (offset > _size)
        return kNotFound;
    return read(offset, entry);
}

} // namespace polar_race