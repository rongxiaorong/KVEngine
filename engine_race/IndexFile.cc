#include "IndexFile.h"

namespace polar_race {

RetCode MemIndex::insert(const char* key, const int &num, const unsigned long offset) {
    std::lock_guard<std::mutex> guard(_write_mtx);
    
}


} // namespace polar_race