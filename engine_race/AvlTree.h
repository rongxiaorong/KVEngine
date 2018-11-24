#ifndef _ENGINE_RACE_AVLTREE_H_
#define _ENGINE_RACE_AVLTREE_H_

// #include "engine_race.h"
// #include "../include/engine.h"
#include "DataCache.h"

namespace polar_race {

template<class T>
struct AvlNode;

template<class T>
struct NodePointer {
    int pointer;
    static AvlNode<T>* head;
    NodePointer<T>(const int &_pointer) : pointer(_pointer){}
    NodePointer<T>(){}
    NodePointer<T>(const AvlNode<T>* _pointer) {
        pointer = _pointer - head;
    }
    AvlNode<T>* operator ->() {
        return &head[pointer];
    }
    operator AvlNode<T>*() {
        return pointer == -1 ? nullptr : &head[pointer];
    }
    NodePointer<T>& operator =(const AvlNode<T>* _pointer) {
        pointer = _pointer - head;
        return *this;
    }
    bool isnull() {
        return pointer == -1;
    }
};

#pragma pack(1)
template<class T>
struct AvlNode {
    T data;
    unsigned short height;
    NodePointer<T> left;
    NodePointer<T> right;
    AvlNode<T>(const T &_data) : data(_data), left(-1), right(-1), height(0){}
    AvlNode<T>(){}
    void init(const T &_data) {
        data = _data;
        left = -1;
        right = -1;
        height = 0;
    }
};
#pragma pack(0)

template <class T>
class AvlTree {
public:
    AvlTree<T>(){}
    ~AvlTree<T>(){}

    void init(int n);
    AvlNode<T>* insert(const T &key);
    AvlNode<T>* find(const T &key);
    AvlNode<T>* minimum();
    AvlNode<T>* maximum();
    int height();
    void foreach(void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor);
    void foreach(const T &beign, const T &end, void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor);
    AvlNode<T>* data() {
        return nodes;
    }
    int size() {
        return count;
    }
    unsigned long getRoot() {
        return root - nodes; 
    }
    void setRoot(unsigned long n) {
        root = nodes + n;
    }
private:
    AvlNode<T>* nodes = nullptr;
    int count = 0;
    AvlNode<T>* root = nullptr;

    AvlNode<T>* newNode(const T & x);
    
    AvlNode<T>* minimum(AvlNode<T>* tree);
    AvlNode<T>* maximum(AvlNode<T>* tree);

    AvlNode<T>* leftLeftRotation(AvlNode<T>* k2);
    AvlNode<T>* rightRightRotation(AvlNode<T>* k1);
    AvlNode<T>* leftRightRotation(AvlNode<T>* k3);
    AvlNode<T>* rightLeftRotation(AvlNode<T>* k1);

    AvlNode<T>* insert(AvlNode<T>* tree, const T &key);
    AvlNode<T>* find(AvlNode<T>* tree, const T &key);
    unsigned int max(unsigned int x, unsigned int y) {
        return x > y ? x : y;
    }
    unsigned int height(AvlNode<T>* node);
    void foreach(AvlNode<T>* node, void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor);
    void foreach(AvlNode<T>* node, const T &beign, const T &end, void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor);
};

template <class T>
void AvlTree<T>::init(int n) {
    nodes = (AvlNode<T>*) malloc(sizeof(AvlNode<T>) * n);
    NodePointer<T>::head = nodes;
    count = 0;
}


template <class T>
AvlNode<T>* AvlTree<T>::newNode(const T & x) {
    nodes[count].init(x);
    return &nodes[count++];
}

template <class T>
int AvlTree<T>::height() {
    return height(root);
}


template <class T>
inline unsigned int AvlTree<T>::height(AvlNode<T>* node) {
    return node == nullptr? 0 : node->height;
}

template <class T>
AvlNode<T>* AvlTree<T>::leftLeftRotation(AvlNode<T>* k2) {
    AvlNode<T>* k1;

    k1 = k2->left;
    k2->left = k1->right;
    k1->right = k2;

    k2->height = max(height(k2->left), height(k2->right)) + 1;
    k1->height = max(height(k1->left), k2->height) + 1;

    return k1;
}

template <class T>
AvlNode<T>* AvlTree<T>::rightRightRotation(AvlNode<T>* k1) {
    AvlNode<T>* k2;
    
    k2 = k1->right;
    k1->right = k2->left;
    k2->left = k1;

    k1->height = max(height(k1->left), height(k1->right)) + 1;
    k2->height = max(height(k2->right), k1->height) + 1;

    return k2;
}

template <class T>
AvlNode<T>* AvlTree<T>::leftRightRotation(AvlNode<T>* k3) {
    k3->left = rightRightRotation(k3->left);
    return leftLeftRotation(k3);
}

template <class T>
AvlNode<T>* AvlTree<T>::rightLeftRotation(AvlNode<T>* k1) {
    k1->right = leftLeftRotation(k1->right);
    return rightRightRotation(k1);
}

template <class T>
AvlNode<T>* AvlTree<T>::insert(const T &key) {
    root = insert(root, key);
    return root;
}

template <class T>
AvlNode<T>* AvlTree<T>::insert(AvlNode<T>* tree, const T &key) {
    if (tree == nullptr)
        tree = newNode(key);
    else if (key < tree->data) {
        tree->left = insert(tree->left, key);
        if (height(tree->left) - height(tree->right) == 2) {
            if (key < tree->left->data)
                tree = leftLeftRotation(tree);
            else
                tree = leftRightRotation(tree);
        }
    }
    else if (key > tree->data) {
        tree->right = insert(tree->right, key);
        if (height(tree->right) - height(tree->left) == 2) {
            if (key > tree->right->data) 
                tree = rightRightRotation(tree);
            else
                tree = rightLeftRotation(tree);
        }
    }
    else {
        // same key
        tree->data = key;
    }

    tree->height = max(height(tree->left), height(tree->right)) + 1;
    return tree;
}

template <class T>
AvlNode<T>* AvlTree<T>::find(const T &key) {
    return find(root, key);
}

template <class T>
AvlNode<T>* AvlTree<T>::find(AvlNode<T>* tree, const T &key) {
    while(true) {
        if (tree == nullptr)
            return nullptr;
        else if (key < tree->data)
            tree = tree->left;
        else if (key > tree->data)
            tree = tree->right;
        else
            return tree;
    }
    return nullptr;
}

template <class T>
AvlNode<T>* AvlTree<T>::minimum() {
    return minimum(root);
}

template <class T>
AvlNode<T>* AvlTree<T>::minimum(AvlNode<T>* tree) {
    if (tree == nullptr)
        return nullptr;
    while(!tree->left.isnull()) {
        tree = tree->left;
    }
    return tree;
}

template <class T>
AvlNode<T>* AvlTree<T>::maximum() {
    return maximum(root);
}

template <class T>
AvlNode<T>* AvlTree<T>::maximum(AvlNode<T>* tree) {
    if (tree == nullptr)
        return nullptr;
    while(!tree->right.isnull()) {
        tree = tree->right;
    }
    return tree;
}

template <class T>
void AvlTree<T>::foreach(const T &begin, const T &end, void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor) {
    foreach(root, begin, end, func, visitor);
}

template <class T>
void AvlTree<T>::foreach(AvlNode<T>* node, const T &begin, const T &end, void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor) {
    if (node == nullptr)
        return;
    foreach(node->left, begin, end, func, visitor);
    if ((node->data > begin || node->data == begin) && node->data < end)
        func(node->data, visitor);
    foreach(node->right, begin, end, func, visitor);
}

template <class T>
void AvlTree<T>::foreach(void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor) {
    foreach(root, func, visitor);
}

template <class T>
void AvlTree<T>::foreach(AvlNode<T>* node, void (*func)(const T &key, RaceVisitor &visitor), RaceVisitor &visitor) {
    if (node == nullptr)
        return;
    foreach(node->left, func, visitor);
    func(node->data, visitor);
    foreach(node->right, func, visitor);
}

} // namespace polar_race
#endif // _ENGINE_RACE_AVLTREE_H_