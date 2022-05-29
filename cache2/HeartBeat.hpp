#ifndef HEARTBEAT_HPP
#define HEARTBEAT_HPP
#include "cache_server.hpp"
using namespace std;

void *task_connectoth(void *arg)
{
    pthread_mutex_lock(&transfertooth);
    struct task_connect_oth* taskconnect((struct task_connect_oth*) arg);
    pthread_mutex_unlock(&transfertooth);

    auto ipport_s = get_ipport(taskconnect->addr);
    string& ip_oth = ipport_s.ip;
    int& port_oth = ipport_s.port;
    int cachfd = Socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in cache_oth_addr;
    bzero(&cache_oth_addr,sizeof(cache_oth_addr));
    cache_oth_addr.sin_family = AF_INET;
    inet_pton(AF_INET, (const char*)ip_oth.data(), &cache_oth_addr.sin_addr.s_addr);
    cache_oth_addr.sin_port = htons(port_oth);
    int ret = connect(cachfd, (struct sockaddr *)&cache_oth_addr, sizeof(cache_oth_addr));
    auto flags = fcntl(cachfd,F_GETFL,0);
    fcntl(cachfd, F_SETFL, flags & O_NONBLOCK);
    if(ret<0)
        return nullptr;

    for(int j=0; j<(*taskconnect->ipport_and_key)[taskconnect->addr].size(); j++)
    {
        string key = (*taskconnect->ipport_and_key)[taskconnect->addr][j];
        string value = (*taskconnect->LC).get(key, false);
        json write_j = Write_oth_ClientCache(key, value);
        string buf = write_j.dump()+"\0";
        int n=Write(cachfd, (const char *)buf.data(), buf.length()+1);
        if(n==0)
        {
            Close(cachfd);
            return nullptr;
        }
        Sleep_ms(100);
    }
    Close(cachfd);

    return nullptr;
}
void* doit_iplistchange(void *arg)
{
    pthread_rwlock_wrlock(&shutdown_lock);
    struct iplistchange_struct* ipchange=(struct iplistchange_struct *)arg;
    shared_ptr<unordered_map<string,vector<string>>> ipport_and_key=make_shared<unordered_map<string,vector<string>>>();
    (*ipchange->key_addr).RefreshIPList(*ipchange->ipport_list);
    if(ipchange->exit_flag)
        cout<<"prepare to close..."<<endl;

    for(auto i = (*ipchange->LC).cache.begin(); i!=(*ipchange->LC).cache.end();i++){
        string key = i->first;
        string addr = (*ipchange->key_addr).GetServerIndex(key);
        (*ipport_and_key)[addr].push_back(key);
    }

    signal(SIGPIPE, SIG_IGN);
    struct task_connect_oth task_arg;
    task_arg.ipport_and_key = ipport_and_key;
    task_arg.LC = ipchange->LC;

    pthread_t takeout_allval[ipport_and_key->size()];
    int count = 0;
    int flag = -1;
    for(auto i = ipport_and_key->begin(); i!=ipport_and_key->end(); i++)
    {
        Sleep_ms(100);
        if(curen_addr==i->first)
        {
            flag = count++;
            continue;
        }
        pthread_mutex_lock(&transfertooth);
        task_arg.addr = i->first;
        Pthread_create(&takeout_allval[count++], nullptr, &task_connectoth, &task_arg);
        pthread_mutex_unlock(&transfertooth);
    }
    Sleep_ms(200);
    for(int i=0; i<ipport_and_key->size(); i++){
        if(flag==i)
            continue;
        pthread_join(takeout_allval[i], nullptr);
    }
    if(ipchange->exit_flag)
    {
        cout<<"close done"<<endl;
        exit(0);
    }

    pthread_rwlock_unlock(&shutdown_lock);
    return nullptr;
}
class Heartbeat
{
private:
    pthread_t tid_beforeshutdown;
    pthread_t tid_askfromothercache;
    shared_ptr<ConsistentHash> key_addr;
    shared_ptr<vector<string>> ipport_list;
    shared_ptr<unordered_map<string ,vector<string>>> sendout_bk;
    pthread_mutex_t ipchange_lock;
    shared_ptr<ThreadPool> ThrPl;
    shared_ptr<LRUCache> LC;
    shared_ptr<LRUCache> LC_BK;
    struct sockaddr_in serv_addr;
    json heartbeat_json, iplist_json;
    char buf_rdlist[BUFSIZ];
    string buf;
    int cfd;
    struct iplistchange_struct ipchange_arg;
public:
    task_connect_oth task_arg;
    pthread_t takeout_allval[200];
    Heartbeat(struct heartbeat_struct *heartbeat_arg);
    ~Heartbeat();
    void heartbeat_connect(bool flag);
    void heartbeat_send();
    int is_anycommond();
    void shutdown_handler();
    void add_handler();
    void reflesh_handler();
};

Heartbeat::Heartbeat(struct heartbeat_struct *heartbeat_arg)
{
    ipchange_lock = PTHREAD_MUTEX_INITIALIZER;
    sendout_bk = make_shared<unordered_map<string ,vector<string>>>();
    key_addr = heartbeat_arg->key_addr;
    ipport_list = heartbeat_arg->ipport_list;
    ThrPl = heartbeat_arg->ThrPl;
    LC = heartbeat_arg->LC;
    LC_BK = heartbeat_arg->LC_BK;
    heartbeat_json = pack_json_heartbeat();
    buf = heartbeat_json.dump();
}
Heartbeat::~Heartbeat()
{
    pthread_mutex_unlock(&ipchange_lock);
    pthread_mutex_destroy(&ipchange_lock);
    pthread_mutex_destroy(&transfertooth);
    pthread_mutex_destroy(&reflesh_master_lock);
    pthread_rwlock_destroy(&shutdown_lock);
    Close(cfd);
}
void Heartbeat::heartbeat_connect(bool flag)
{
    int n=-1;
    do{
        if(flag)
        {
            Close(cfd);
            Sleep_ms(100);
        }
        signal(SIGPIPE, SIG_IGN);
        bzero(&serv_addr,sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        pthread_mutex_lock(&reflesh_master_lock);
        auto m_addr = get_ipport(master_addr);
        string& MASTER_IP = m_addr.ip;
        int& MASTER_PORT = m_addr.port;
        pthread_mutex_unlock(&reflesh_master_lock);
        inet_pton(AF_INET, (char *)MASTER_IP.data(), &serv_addr.sin_addr.s_addr);
        serv_addr.sin_port = htons(MASTER_PORT);
        auto info_con = Socket_connect((struct sockaddr *)&serv_addr, sizeof(serv_addr));
        cfd = info_con.cfd;
        n = info_con.n;
        flag = true;
    }while(n<0);
    auto flags = fcntl(cfd,F_GETFL,0);
    fcntl(cfd,F_SETFL,flags|O_NONBLOCK);
}
void Heartbeat::heartbeat_send()
{
    int n;
    buf += "\0";
    n = Write(cfd, (char *)buf.data(), buf.length()+1);
    if(n<0){
        heartbeat_connect(true);
    }
}
int Heartbeat::is_anycommond()
{
    int m=0;
    do{
        m = unblock_read_net(cfd, buf_rdlist, BUFSIZ);
        if(0<m){
            if(!(json::accept(buf_rdlist)))
                continue;
            iplist_json = json::parse(buf_rdlist);
            return (int)iplist_json["type"];
        }
        else if(0==m){
            heartbeat_connect(true);
        }
    }while(0==m);
    return -1;
}
void Heartbeat::shutdown_handler()
{
    pthread_mutex_lock(&ipchange_lock);
    (*ipport_list).clear();
    for(int i=0;i<iplist_json["data"]["iplist"].size();i++){
        (*ipport_list).push_back(iplist_json["data"]["iplist"][i]);
    }
    ipchange_arg.ThrPl = ThrPl;
    ipchange_arg.LC = LC;
    ipchange_arg.exit_flag = true;
    ipchange_arg.key_addr = key_addr;
    ipchange_arg.ipport_list = ipport_list;
    Pthread_create(&tid_beforeshutdown, nullptr, doit_iplistchange, &ipchange_arg);
    pthread_mutex_unlock(&ipchange_lock);
}
void Heartbeat::add_handler()
{
    pthread_mutex_lock(&ipchange_lock);
    (*ipport_list).clear();
    for(int i=0;i<iplist_json["data"]["iplist"].size();i++){
        (*ipport_list).push_back(iplist_json["data"]["iplist"][i]);
    }
    ipchange_arg.ThrPl = ThrPl;
    ipchange_arg.LC = LC;
    ipchange_arg.exit_flag = false;
    ipchange_arg.key_addr = key_addr;
    ipchange_arg.ipport_list = ipport_list;
    Pthread_create(&tid_askfromothercache, nullptr, doit_iplistchange, &ipchange_arg);
    pthread_mutex_unlock(&ipchange_lock);
}
void Heartbeat::reflesh_handler()
{
    pthread_mutex_lock(&ipchange_lock);
    (*ipport_list).clear();
    unordered_set<string> ipnew;
    vector<string> key_send;
    sendout_bk->clear();

    for(int i=0; i<iplist_json["data"]["iplist"].size(); i++){
        (*ipport_list).push_back(iplist_json["data"]["iplist"][i]);
        ipnew.insert((string)iplist_json["data"]["iplist"][i]);
    }
    for(auto i = (*LC_BK).cache.begin(); i!=(*LC_BK).cache.end();i++){
        string key = i->first;
        string addr = (*key_addr).GetServerIndex(key);
        if(ipnew.find(addr)==ipnew.end()){
            key_send.push_back(key);
        }
    }
    (*key_addr).RefreshIPList(*ipport_list);
    for(int i=0; i<key_send.size(); i++){
        (*sendout_bk)[(*key_addr).GetServerIndex(key_send[i])].push_back(key_send[i]);
    }
    if(!(sendout_bk->empty())){
        signal(SIGPIPE, SIG_IGN);
        int count=0;        
        task_arg.LC = LC_BK;
        task_arg.ipport_and_key = sendout_bk;
        int flag=-1;
        for(auto i = sendout_bk->begin(); i!=sendout_bk->end(); i++){
            if(curen_addr==i->first){
                for(int j=0; j<(*sendout_bk)[curen_addr].size(); j++){
                    string temp_key = (*sendout_bk)[curen_addr][j];
                    string temp_val = LC_BK->get(temp_key, false);
                    LC->put(temp_key, temp_val);
                }
                flag = count++;
                continue;
            }
            Sleep_ms(100);
            pthread_mutex_lock(&transfertooth);
            task_arg.addr = i->first;
            Pthread_create(&takeout_allval[count++], nullptr, &task_connectoth, &task_arg);
            pthread_mutex_unlock(&transfertooth);        
        }
    }
    pthread_mutex_unlock(&ipchange_lock);
}

void *heart_beat(void *arg)
{
    Heartbeat heartbeat_hd((struct heartbeat_struct *)arg);
    heartbeat_hd.heartbeat_connect(false);
    while(1){
        heartbeat_hd.heartbeat_send();
        Sleep_ms(100);
        int commond = heartbeat_hd.is_anycommond();
        if(SHUTDOWN_CACHE==commond){
            heartbeat_hd.shutdown_handler();
            break;
        }
        else if(ADD_CACHE==commond){
            heartbeat_hd.add_handler();
        }
        else if(REFLESH_IP==commond){
            heartbeat_hd.reflesh_handler();
        }
    }
    while(1){
        sleep(100);
    }
    return nullptr;
}
#endif /* HEARTBEAT_HPP */
