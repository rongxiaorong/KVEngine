// BloomFilter.cc
#include "include/BloomFilter.h"
#include <memory.h>

namespace polar_race {
    BloomFilter::BloomFilter(int entry_size  /* m */, int entry_inserts  /* n */) {
    	this->init(entry_size,entry_inserts);
	}
	void BloomFilter::init(int entry_size  /* m */, int entry_inserts  /* n */) {
		// k = ln2 * (m/n)
    	this->bitsetSize = entry_size;
		this->sumHashFunction = (int)((double)entry_size / (double)entry_inserts * ln2);
		this->sumHashFunction %= maxFunctions;
		bool* temp = new bool[this->bitsetSize];
		memset(temp, false, sizeof(bool) * this->bitsetSize);  // set as 0
		this->dataPointer = temp;
	}

    void BloomFilter::set(const std::string src) {
    	std::bitset<inputSize / 2> left(0), right(0);
    	this->preProcess(&left, &right, src);
		for(int i = 1; i <= this->sumHashFunction; ++i)
		{
			int pos = this->dynamicHash(&left, &right, i);
			this->dataPointer[pos] = true;
		}
	}
    
    bool BloomFilter::get(const std::string src) {
    	std::bitset<inputSize / 2> left(0), right(0);
    	this->preProcess(&left, &right, src);
		for(unsigned int i = 1; i <= this->sumHashFunction; ++i)
		{
			unsigned int pos = this->dynamicHash(&left, &right, i);
			if(!(this->dataPointer[pos]))
				return false;
		}
		return true;
	}

    bool* BloomFilter::data() {
		return this->dataPointer;
	
	}
	
	unsigned int BloomFilter::size() {
		return this->bitsetSize;
		
	}
	
	unsigned int BloomFilter::dynamicHash(std::bitset<inputSize / 2>* left, std::bitset<inputSize / 2>* right,  unsigned int salt) {
		unsigned int a = left->to_ulong();
		unsigned int b = right->to_ulong();
		// add salt--> extend
		a = a + b * salt;  // mod 2^32
		return (a % this->bitsetSize);
		
	}
	
	void BloomFilter::preProcess(std::bitset<inputSize / 2>* left, std::bitset<inputSize / 2>* right, const std::string src) {
		// to 8B
    	int length = inputSize / 8;  // number of bytes
    	if(src.size() < length)
    		length = src.size();
    	memcpy(left, src.c_str(), length / 2);
    	memcpy(right, src.c_str() + length / 2, length / 2 + length % 2);
	}

} // namespace polar_race
