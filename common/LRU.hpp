#ifndef LRU
#define LRU

#include "string"
#include <unordered_map>
#include <vector>
using namespace std;

//带前后指针的节点
struct DLinkedNode
{
    string key,value;
    DLinkedNode* prev;
    DLinkedNode* next;
    DLinkedNode():key(20,' '),value(20,' '),prev(nullptr),next(nullptr){}
    DLinkedNode(string _key,string _value):key(_key),value(_value),prev(nullptr),next(nullptr){}
};

class LRUCache
{
private:
    int capacity;
    void addToHead(DLinkedNode* node);
    void removeNode(DLinkedNode* node);
    void moveToHead(DLinkedNode* node);
    DLinkedNode* removeTail();
public:
    unordered_map<string,DLinkedNode*> cache;
    vector<string> key_vec;
    DLinkedNode* head;
    DLinkedNode* tail;
    int size;
    
    LRUCache(int _capacity);
    string get(string key,bool flag);
    void put(string key,string value);
};
LRUCache::LRUCache(int _capacity):capacity(_capacity),size(0)
{
    head=new DLinkedNode();
    tail=new DLinkedNode();
    head->next=tail;
    tail->prev=head;
}
string LRUCache::get(string key,bool flag=true)
{
    if(!cache.count(key))
        return "";
    DLinkedNode* node=cache[key];
    if(flag)
        moveToHead(node);
    return node->value;
}
void LRUCache::put(string key,string value)
{
    if(!cache.count(key))
    {
        DLinkedNode* node=new DLinkedNode(key,value);
        cache[key]=node;
        addToHead(node);
        size++;
        if(size>capacity)
        {
            DLinkedNode* removed=removeTail();
            cache.erase(removed->key);
            delete removed;
            size--;
        }
    }
    else
    {
        DLinkedNode* node=cache[key];
        node->value=value;
        moveToHead(node);
    }
}
void LRUCache::addToHead(DLinkedNode* node)
{
    node->prev=head;
    node->next=head->next;
    head->next->prev=node;
    head->next=node;
}
void LRUCache::removeNode(DLinkedNode* node)
{
    node->prev->next=node->next;
    node->next->prev=node->prev;
}
void LRUCache::moveToHead(DLinkedNode* node)
{
    removeNode(node);
    addToHead(node);
}
DLinkedNode* LRUCache::removeTail()
{
    DLinkedNode* node=tail->prev;
    removeNode(node);
    return node;
}



#endif