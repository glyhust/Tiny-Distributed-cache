#ifndef CON_HASH
#define CON_HASH

#include <cstdlib>
#include <sstream>
#include <iostream>
#include <ctime>
#include <string.h>
#include <vector>
#include <set>
#include <map>
using namespace std;

class ConsistentHash
{
private:
    map<uint32_t,string> virtualNodes;
    map<uint32_t,string> serverNodes;
    int virtualNodeNum;
public:
    ConsistentHash(int num=200):virtualNodeNum(num){}
    ~ConsistentHash(){}

    static uint32_t GETHash(string key);
    bool RefreshIPList(const vector<string>& iplist);
    bool AddServer(const string& nodeIP);
    string GetServerIndex(const string& key);
    string GetBackUpServer(const string& key);
};

uint32_t ConsistentHash::GETHash(string key)
{
    const int p=16777619;
    uint32_t hash=2166136261;
    for(int index=0;index<key.size();index++)
    {
        hash=(hash^key[index])*p;
    }
    hash+=hash<<13;
    hash^=hash>>7;
    hash+=hash<<3;
    hash^=hash>>17;
    hash+=hash<<5;
    if(hash<0)
        hash=-hash;
    return hash;
}
bool ConsistentHash::AddServer(const string& nodeIP)
{
    for(int i=0;i<virtualNodeNum;i++)
    {
        stringstream nodeKey;
        nodeKey<<nodeIP<<"#"<<i;
        uint32_t partition=GETHash(nodeKey.str());
        virtualNodes.insert({partition,nodeIP});
    }
    uint32_t partition=GETHash(nodeIP);
    serverNodes.insert({partition,nodeIP});
    return true;
}
bool ConsistentHash::RefreshIPList(const vector<string>& iplist)
{
    virtualNodes.clear();
    serverNodes.clear();
    bool f=true;
    for(auto& ip:iplist)
    {
        f=f&&AddServer(ip);
    }
    return f;
}
string ConsistentHash::GetServerIndex(const string& key)
{
    uint32_t partition=GETHash(key);
    auto it=virtualNodes.lower_bound(partition);
    if(it==virtualNodes.end())
    {
        if(virtualNodes.empty())
            cout<<"no available nodes"<<'\n';
        return virtualNodes.begin()->second;
    }
    return it->second;
}
string ConsistentHash::GetBackUpServer(const string& key)
{
    uint32_t partition=GETHash(key);
    auto it=virtualNodes.lower_bound(partition);
    if(it==virtualNodes.end())
        it=virtualNodes.begin();
    while(it!=virtualNodes.end())
    {
        it++;
        if(it==virtualNodes.end())
            it=virtualNodes.begin();
        if(it->second!=GetServerIndex(key))
            break;
    }
    return it->second;
}

#endif