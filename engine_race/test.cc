#include <iostream>
// #include "include/util.h"
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include "engine_race.h"
using namespace std;
// using namespace polar_race;
// #pragma pack(1)

using namespace polar_race;
string valueGenerator(char x) {
    string result(&x, 1);
    for (int i = 0; i < 12; i++)
        result = result + result;
    return result;
}
int main() {
    Engine* engine;
    EngineRace::Open("data", &engine);
    engine->Write("ABCDEFGH", valueGenerator('a'));
    string result = "ab";
    engine->Read("ABCDEFGH", &result);
    cout << result;
    return 0;
}