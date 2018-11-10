// #include <iostream>
// // #include "include/util.h"
// #include <unistd.h>
// #include <stdio.h>
// #include <fcntl.h>
// #include <thread>
// #include <mutex>
// #include <atomic>
// #include <condition_variable>
// #include "engine_race.h"
// using namespace std;
// // using namespace polar_race;
// // #pragma pack(1)

// using namespace polar_race;
// string valueGenerator(char x) {
//     string result(&x, 1);
//     for (int i = 0; i < 12; i++)
//         result = result + result;
//     return result;
// }
// string keyGenerator(char x) {
//     string result(&x, 1);
//     for (int i = 0; i < 3; i++)
//         result = result + result;
//     return result;
// }
// int main() {
//     Engine* engine;
//     EngineRace::Open("/home/francis/Git/KVEngine/engine_race/build/data", &engine);
//     cout << "checking" << endl;
//     // for (char i = 32; i < 127; i ++)
//     //     engine->Write(keyGenerator(i), valueGenerator(i));
//     // for (char i = 32; i < 100; i ++)
//     //     engine->Write(keyGenerator(i), valueGenerator(i));
//     // for (char i = 32; i < 100; i ++)
//     //     engine->Write(keyGenerator(i), valueGenerator(i));
//     string result = "ab";
//     engine->Write(keyGenerator(32), valueGenerator(32));
//     engine->Read(keyGenerator(32),&result);
//     cout << (int)result[0] << endl;
//     engine->Write(keyGenerator(32), valueGenerator(82));
//     engine->Read(keyGenerator(32),&result);
//     cout << (int)result[0] << endl;
//     // cout << "finish" << endl;
//     // while(1);
//     delete engine;
//     // cout << result;
//     return 0;
// }