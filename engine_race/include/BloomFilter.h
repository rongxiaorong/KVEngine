// BloomFilter.h
#ifndef _ENGINE_RACE_BLOOMFILTER_H_
#define _ENGINE_RACE_BLOOMFILTER_H_

#include <string>
#include <bitset>

const int inputSize = 64;  // 8B
const double ln2 = 0.693147;
const int maxFunctions = 100;  // changeable 


namespace polar_race {
class BloomFilter {
public:
    // size (bits) for an entry
    BloomFilter(){};

    BloomFilter(int entry_size  /* m */, int entry_inserts  /* n */);

    void init(int entry_size  /* m */, int entry_inserts  /* n */);
    
    void set(const std::string src);
    
    bool get(const std::string src);

    bool* data();

    // size of the array (bytes)
    unsigned int size();
private:
	bool* dataPointer;
	unsigned int bitsetSize;  // actually, it is a bool array
	int sumHashFunction;
	
	unsigned int dynamicHash(std::bitset<inputSize / 2>* left, std::bitset<inputSize / 2>* right,  unsigned int salt);
	
	void preProcess(std::bitset<inputSize / 2>* left, std::bitset<inputSize / 2>* right, const std::string src);  // length control, just ignore it
};  
}

#endif // _ENGINE_RACE_BLOOMFILTER_H_
