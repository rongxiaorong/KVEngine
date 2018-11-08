#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
int main() {
    int fd = open("abc.txt",O_CREAT|O_WRONLY,0777);
    char buf[5] = "123";
    write(fd,buf,5);
    ::remove("abc.txt");
}
