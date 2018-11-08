#include <iostream>
// #include "include/util.h"
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

using namespace std;
// using namespace polar_race;
// #pragma pack(1)
struct IndexCacheEntry {
    char key[8];
    unsigned short fid;
    size_t offset;
};
struct IndexEntry{
    char k[8];
    size_t p;
};
extern int a;
int a = 3;

int main() {
    cout << sizeof(IndexEntry);
}