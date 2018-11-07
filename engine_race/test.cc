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

atomic_long count;
bool ismut = false;
mutex mtx1;
mutex table_mtx;
void run1() {
  
}

bool write() {
  lock_guard<mutex> guard(mtx);
  count++;
  if (count > 10)
    return table_immut();
  return false;
}
bool table_immut() {
  lock_guard<mutex> guard(table_mtx);
  if (ismut)
    return true;
  ismut = true;
  
}
void run2() {

}
int main() {
    
}