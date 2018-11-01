// BloomFilter.h
#ifndef _ENGINE_RACE_BLOOMFILTER_H_
#define _ENGINE_RACE_BLOOMFILTER_H_

#include <string>

namespace polar_race {
class BloomFilter {
public:
    // size (bits) for an entry
    BloomFilter(int entry_size);

    void set(const std::string src);
    
    bool get(const std::string src);

    void* data();

    // size of the array (bytes)
    int size();
};  
}

#endif // _ENGINE_RACE_BLOOMFILTER_H_