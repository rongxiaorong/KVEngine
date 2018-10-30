// allocator.h
#ifndef _HMDB_ALLOCATOR_H_
#define _HMDB_ALLOCATOR_H_

#include <unistd.h>
#include "config.h"
#include "common.h"

namespace polar_race {
class MemAllocator {
public:
    static size_t total_used;
    MemAllocator(size_t block_size) {
        _init(block_size);
    }
    void* allocate(size_t bytes) {
        if (_on_using->_cur + bytes > _on_using->_size) {
            // need a new allocator
            _on_using->_next = (MemAllocator*)(_on_using->_ptr + _on_using->_size);
            _on_using = _on_using->_next;
            if (bytes > _size) 
                // new a bigger area
                _on_using->_init(bytes);
            else
                _on_using->_init(_size);
        }
        return _on_using->_allocate(bytes);
    }
    size_t size() {return _size;}
    size_t remain() {return _size - _cur;}
    void clear() {_cur = 0;};
    ~MemAllocator() {
        _delete();
    };
private:
    MemAllocator() {}
    void* _allocate(size_t bytes) {
        if (_cur + bytes > _size)
            ERROR("Fucking _allocate out of range._cur=%ul bytes=%ul _size=%ul", _cur, bytes, _size);
        void* res = _ptr + _cur;
        _cur += bytes;
        return res; 
    }
    void _init(size_t block_size) {
        _ptr = new char[block_size + sizeof(MemAllocator)];
        _size = block_size;
        _cur = 0;
        _next = nullptr;
        _on_using = this;
        total_used += _size;
    }
    void _delete() {
        if (_next != nullptr)
            _next->_delete();
        delete _ptr;
        total_used -= _size;
    }
    size_t _size;
    size_t _cur;
    char* _ptr;
    MemAllocator* _next;
    MemAllocator* _on_using;
};

} // namespace polar_race
#endif // _HMDB_ALLOCATOR_H_

