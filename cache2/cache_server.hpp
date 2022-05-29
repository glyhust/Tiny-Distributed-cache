#ifndef CACHE_SERVER_HPP
#define CACHE_SERVER_HPP

#include <cstring>
#include <cctype>
#include <fcntl.h>
#include <unordered_set>
#include <signal.h>
#include "../common/wrap_fun.hpp"
#include "../common/json.hpp"
#include "../common/LRU.hpp"
#include "../common/threadpool.hpp"
#include "../common/con_hash.hpp"


#define KEY_VALUE_REQUEST 0
#define KEY_VALUE_RESPOND 1
#define HEART_BEAT 2
#define SHUTDOWN_CACHE 6
#define ADD_CACHE 7
#define REFLESH_MASTER 9
#define REFLESH_IP 10
#define KEY_VALUE_RESPONDBK 11
#define CACHESERV_IP "127.0.0.1"
#define CACHESERV_PORT 8001
string master_addr = "127.0.0.1:7000";
const string curen_addr = CACHESERV_IP+(string)":"+to_string(CACHESERV_PORT);
long long request_count = 0;
long long rcv_cli = 0;
long long rcv_bk = 0;
using json = nlohmann::json;
using namespace std;
struct heartbeat_struct
{
    shared_ptr<LRUCache> LC;
    shared_ptr<LRUCache> LC_BK;
    shared_ptr<ThreadPool> ThrPl;
    shared_ptr<ConsistentHash> key_addr;
    shared_ptr<vector<string>> ipport_list;
};

struct pack_taskconnect
{
    int sockfd;
    int efd;
    char* buf;
    shared_ptr<LRUCache> LC;
    shared_ptr<LRUCache> LC_BK;
    shared_ptr<ConsistentHash> key_addr;
};

struct pack_tasklisten
{
    struct sockaddr_in *cliaddr;
    int listenfd;
    int num;
    int efd;
};
struct iplistchange_struct
{
    int cfd;
    bool exit_flag;
    shared_ptr<LRUCache> LC;
    shared_ptr<ThreadPool> ThrPl;
    shared_ptr<ConsistentHash> key_addr;
    shared_ptr<vector<string>> ipport_list;
};
struct task_connect_oth
{
    string addr;
    shared_ptr<unordered_map<string, vector<string>>> ipport_and_key;
    shared_ptr<LRUCache> LC;
};
//////////////////////////////
pthread_mutex_t transfertooth = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t task_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t tasklisten_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t rw_lock = PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t reflesh_master_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t shutdown_lock = PTHREAD_RWLOCK_INITIALIZER;
//////////////////////////

json Write_oth_ClientCache(const string& key, const string& value)
{
    json client_to_cache, data;
    data["flag"] = true;
    data["key"] = key;
    data["value"] = value;
    client_to_cache["type"] = KEY_VALUE_RESPOND;
    client_to_cache["data"] = data;
    return client_to_cache;
}
json Write_bk_ClientCache(const string& key, const string& value)
{
    json client_to_cache, data;
    data["flag"] = true;
    data["key"] = key;
    data["value"] = value;
    client_to_cache["type"] = KEY_VALUE_RESPONDBK;
    client_to_cache["data"] = data;
    return client_to_cache;
}
json pack_json_heartbeat()
{
    json data, heartbeat_json;
    string number = curen_addr;
    data["iplist"] = number;
    data["state"] = true;
    heartbeat_json["type"] = HEART_BEAT;
    heartbeat_json["data"] = data;
    return heartbeat_json;
}

#endif /* CACHE_SERVER_HPP */
