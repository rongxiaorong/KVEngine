// BloomFilter.h
#ifndef _ENGINE_RACE_BLOOMFILTER_H_
#define _ENGINE_RACE_BLOOMFILTER_H_

#include <string>
#include <bitset> 

namespace polar_race {

class BloomFilter {
public:
    // size (bits) for an entry
	BloomFilter(){}
	~BloomFilter(){this->clear();}
    BloomFilter(int entry_size  /* m */, int entry_inserts  /* n */);
	void init(int entry_size  /* m */, int entry_inserts  /* n */, void* buf);
    void set(const std::string src);
    
    bool get(const std::string src);

	void clear() {
		if (dataPointer != nullptr)
			delete dataPointer;
		dataPointer = nullptr;
	}
    unsigned char* data();

    // size of the array (bytes)
    unsigned int size();
private:
	static const int inputSize = 64;  // 8B
	static const constexpr double ln2 = 0.693147;
	static const int maxFunctions = 100;  // changeable
	static const unsigned int blockLen = 8;
	unsigned char* dataPointer = nullptr;
	unsigned int bitsetSize;  // actually, it is a bool array
	int sumHashFunction;
	// dynamic_bitset ops
	unsigned char* bitArray(unsigned int array_size);
	bool getBit(unsigned char* bits, unsigned int index);
	void setBit(unsigned char* bits, unsigned int index, unsigned char value);
	
	unsigned int dynamicHash(std::bitset<inputSize / 2>* left, std::bitset<inputSize / 2>* right,  unsigned int salt);
	
	void preProcess(std::bitset<inputSize / 2>* left, std::bitset<inputSize / 2>* right, const std::string src);  // length control, just ignore it
};  
}

#endif // _ENGINE_RACE_BLOOMFILTER_H_
