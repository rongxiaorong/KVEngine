#include <iostream>
#include <stdio.h>
// // #define nullpte NULL

// template<class T>
// struct AvlNode;

// template<class T>
// struct NodePointer {
//     int pointer;
//     static AvlNode<T>* head;
//     NodePointer<T>(const int &_pointer) : pointer(_pointer){}
//     NodePointer<T>(){}
//     NodePointer<T>(const AvlNode<T>* _pointer) {
//         pointer = _pointer - head;
//     }
//     AvlNode<T>* operator ->() {
//         return &head[pointer];
//     }
//     operator AvlNode<T>*() {
//         return pointer == -1 ? NULL : &head[pointer];
//     }
//     NodePointer<T>& operator =(const AvlNode<T>* _pointer) {
//         pointer = _pointer - head;
//         return *this;
//     }
//     bool isnull() {
//         return pointer == -1;
//     }
// };

// #pragma(1)
// template<class T>
// struct AvlNode {
//     T data;
//     unsigned short height;
//     NodePointer<T> left;
//     NodePointer<T> right;
//     AvlNode<T>(const T _data) : data(_data), left(-1), right(-1), height(0){}
//     AvlNode<T>(){}
// };
// #pragma(0)

// int getData(AvlNode<int>* p) {
//     return p->data;
// }
// template <typename T>
// AvlNode<T>* NodePointer<T>::head = NULL;

// class A {
// public:
//     int x = 0;
//     void add(int p) {
//         x += p;
//         std::cout << "x =" << x << "\n";
//     }
// };

// void play(void (*p)(int)) {
//     for (int i = 0;i<3;i++)
//         p(i);
// }
struct fuck {
    char k[8];
    unsigned short n;
    unsigned int offset;
};
int main() {
    sizeof(fuck);
    // using namespace std;
    // A a;
    // play(a.add);
    // char a[] = {'A','B','X'};
    // printf("%x%x%x", a[0], a[1], a[2]);
    // AvlNode<int> n[10];
    // NodePointer<int>::head = n;
    // NodePointer<int> p[10];
    // for(int i = 0;i < 10;i ++)
    //     n[i].data = i + 100;
    // p[0].pointer = 0;
    // p[1].pointer = 1;
    // p[2] = &n[5];
    // cout << p[0]->data << "\n" << getData(p[1]) << "\n" << p[2]->data;
    // int k[10];
    // int* a = k;
    // int* b = &k[5];
    // cout << "\n" << b - a;
}
