#include <iostream>
#include "engine_race.h"
#include "IndexFile.h"
using namespace polar_race;
using namespace std;

const char* kEnginePath = "/home/francis/Git/KVEngine/engine_race/build/data";

string simpleString(char c, int num) {
    char buf[4096];
    for (int i = 0;i < num; i++)
        buf[i] = c;
    return string(buf, num);
}

void testRange(Engine* engine) {

}

int main(){
    Engine *engine = NULL;
    Engine::Open(kEnginePath, &engine);
    // assert (ret == kSucc);
    for (char i = 'A' + 19; i >= 'A';i--) {    
        string key = simpleString(i, 8);
        string value = simpleString(i, 4096);
        string v;
        // cout << "write " << i << "\n";
        // engine->Write(key,value);

        // engine->Read(PolarString(key), &v);
        // if (value != v)
        //     cout << "Wrong value\n";
        // else
        //     cout << "find " << i << "\n";
    }
    Visitor visitor;
    engine->Range("", "", visitor);
    cout << "ok\n";
    delete engine;
    // cout << sizeof(AvlNode<IndexEntry>);
    // indexWriterCV.notify_all();
    while(1);
}