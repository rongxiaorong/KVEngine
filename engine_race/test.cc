#include <iostream>
#include "engine_race.h"

using namespace polar_race;
using namespace std;

const char* kEnginePath = "/home/francis/Git/KVEngine/engine_race/build/data";

string simpleString(char c, int num) {
    string ret;
    for (int i = 0;i < num; i++)
        ret += c;
    return ret;
}

int main(){
    Engine *engine = NULL;
    RetCode ret = Engine::Open(kEnginePath, &engine);
    // assert (ret == kSucc);
    engine->Write(PolarString(simpleString('A', 8)), PolarString(simpleString('A', 4096)));
}