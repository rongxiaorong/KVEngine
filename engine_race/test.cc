#include <iostream>
#include "include/util.h"
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>

using namespace std;
using namespace polar_race;

int main() {
    WritableFile wf;
    int fd = ::open("test.txt",O_WRONLY|O_CREAT|O_APPEND);
    wf.open(fd);
    // wf.append("abcdefd",7);
    wf.append("123456789\n",10);
    wf.flush(); 
    wf.append("123456789\n",10);
    wf.append("123456789\n",10);
   wf.append("123456789\n",10);
   wf.append("123456789\n",10);
  wf.append("123456789\n",10);
    wf.append("123456789\n",10);
   wf.append("123456789\n",10);
   wf.append("123456789\n",10);
 wf.append("123456789\n",10);
   wf.append("123456789\n",10);
    wf.flush();   
    wf.close();
}