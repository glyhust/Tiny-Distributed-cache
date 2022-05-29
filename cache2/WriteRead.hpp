#ifndef CACHESERVER_HPP
#define CACHESERVER_HPP
#include "cache_server.hpp"

class Task_putget
{
private:
    shared_ptr<LRUCache> LC;
    shared_ptr<LRUCache> LC_BK;
    shared_ptr<ConsistentHash> key_addr;
    int sockfd;
    char buf[BUFSIZ];
    int efd;
    json info;
public:
    Task_putget(struct pack_taskconnect *task_arg);
    ~Task_putget(){}
    ssize_t read_fromclient();
    void closeconnect();
    int is_infotype();
    void kvrequest_handler();
    void kvrespond_handler();
    string refleshmaster();
};
Task_putget::Task_putget(struct pack_taskconnect *task_arg)
{
    LC = task_arg->LC;
    LC_BK = task_arg->LC_BK;
    key_addr = task_arg->key_addr;
    sockfd = task_arg->sockfd;
    efd = task_arg->efd;
}
ssize_t Task_putget::read_fromclient()
{
    ssize_t n = Readonce(sockfd, buf, BUFSIZ);
    return n;
}
void Task_putget::closeconnect()
{
    Epoll_ctl(efd, EPOLL_CTL_DEL, sockfd, nullptr);
    Close(sockfd);
}
int Task_putget::is_infotype()
{
    if(json::accept(buf))
    {
        info = json::parse(buf);
        return (int)info["type"];
    }
    else
        return -1;
}
void Task_putget::kvrequest_handler()
{
    pthread_rwlock_rdlock(&rw_lock);
    info["data"]["value"] = (*LC).get(info["data"]["key"]);
    if(info["data"]["value"]=="")
        info["data"]["flag"] = false;
    else
        info["data"]["flag"] = true;
    string str_out = info.dump()+"\0";
    Write(sockfd, (char *)str_out.data(), str_out.length()+1);
    pthread_rwlock_unlock(&rw_lock);
}
string Task_putget::refleshmaster()
{
    return (string)info["data"]["iplist"];
}
void Task_putget::kvrespond_handler()
{
    pthread_rwlock_wrlock(&rw_lock);
    if(info["type"]==KEY_VALUE_RESPONDBK)
    {
        cout<<"the number of receiving backup data is "<<++rcv_bk<<endl;
        (*LC_BK).put(info["data"]["key"], info["data"]["value"]);
        pthread_rwlock_unlock(&rw_lock);
        return;
    }
    cout<<"the number of receiving stored data is "<<++rcv_cli<<endl;
    (*LC).put(info["data"]["key"], info["data"]["value"]);

    string addr = key_addr->GetBackUpServer(info["data"]["key"]);
    if(!addr.empty() && addr!=curen_addr)
    {
        auto ipport_s = get_ipport(addr);
        string& ip = ipport_s.ip;
        int& port = ipport_s.port;
        int cachfd = Socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in cache_bk_addr;
        bzero(&cache_bk_addr,sizeof(cache_bk_addr));
        cache_bk_addr.sin_family = AF_INET;
        inet_pton(AF_INET, (const char*)ip.data(), &cache_bk_addr.sin_addr.s_addr);
        cache_bk_addr.sin_port = htons(port);
        if(!(connect(cachfd, (struct sockaddr *)&cache_bk_addr, sizeof(cache_bk_addr))<0))
        {
            auto flags = fcntl(cachfd,F_GETFL,0);
            fcntl(cachfd,F_SETFL,flags|O_NONBLOCK);
            json sout_data = Write_bk_ClientCache((const string)info["data"]["key"], (const string)info["data"]["value"]);
            string s_data = sout_data.dump()+"\0";
            Write(cachfd, (char *)s_data.data(), s_data.length()+1);
        }
        Close(cachfd);
    }
    pthread_rwlock_unlock(&rw_lock);
}
void task_cfd(void *arg)
{
    pthread_rwlock_rdlock(&shutdown_lock);
    pthread_mutex_lock(&task_lock);
    Task_putget task_pg((struct pack_taskconnect *)arg);
    pthread_mutex_unlock(&task_lock);
    ssize_t n = task_pg.read_fromclient();
    if(n==0)
        task_pg.closeconnect();
    else if(n>0)
    {
        const int infotype = task_pg.is_infotype();
        if(infotype==KEY_VALUE_REQUEST)
            task_pg.kvrequest_handler();
        else if(infotype==KEY_VALUE_RESPOND||infotype==KEY_VALUE_RESPONDBK)
            task_pg.kvrespond_handler();
        else if(infotype==REFLESH_MASTER)
        {
            pthread_mutex_lock(&reflesh_master_lock);
            master_addr = task_pg.refleshmaster();
            pthread_mutex_unlock(&reflesh_master_lock);
        }
    }
    pthread_rwlock_unlock(&shutdown_lock);
}

void task_listen(void *task_arg)
{
    
    pthread_rwlock_rdlock(&shutdown_lock);
    pthread_mutex_lock(&tasklisten_lock);
    struct pack_tasklisten* task_lst((struct pack_tasklisten *)task_arg);
    pthread_mutex_unlock(&tasklisten_lock);
        
    socklen_t clilen = sizeof(task_lst->cliaddr);
    int connfd = Accept(task_lst->listenfd, (struct sockaddr *)&task_lst->cliaddr, &clilen);
    struct sockaddr_in cliaddr;
    char str[INET_ADDRSTRLEN];
    cout<<"received from "<<inet_ntop(AF_INET, &cliaddr.sin_addr, str, sizeof(str))<<" at PORT "<<ntohs(cliaddr.sin_port)<<endl; 
    
    auto flag = fcntl(connfd, F_GETFL);
    fcntl(connfd, F_SETFL, flag | O_NONBLOCK);
    struct epoll_event tep;
    tep.events = EPOLLIN | EPOLLOUT | EPOLLET;
    tep.data.fd = connfd;
    Epoll_ctl(task_lst->efd, EPOLL_CTL_ADD, connfd, &tep);

    pthread_rwlock_unlock(&shutdown_lock);
}

class WriteRead
{
private:
    const int openmax = 100;
    struct epoll_event ep[100];
    int num = 0, listen_num = 20;
    char buf[BUFSIZ];
    struct sockaddr_in cliaddr;
    struct sockaddr_in servaddr;
    struct epoll_event tep;
    struct pack_tasklisten task_arg_listen;
    struct pack_taskconnect task_arg;
    int efd;
    shared_ptr<ThreadPool> ThrPl;
    int listenfd;
public:
    WriteRead(shared_ptr<ThreadPool> ThrPl, shared_ptr<LRUCache> LC, shared_ptr<LRUCache> LC_BK, shared_ptr<ConsistentHash> key_addr);
    ~WriteRead();
    void WR_listen();
    int WR_listenWait(int microsecond);
    void WR_listenHandler();
    void WR_cfdHandler(int index);
    bool is_listenfd(int index);
};

WriteRead::WriteRead(shared_ptr<ThreadPool> ThrPl, shared_ptr<LRUCache> LC, shared_ptr<LRUCache> LC_BK,  shared_ptr<ConsistentHash> key_addr)
{
    task_arg.LC = LC;
    task_arg.LC_BK = LC_BK;
    this->ThrPl = ThrPl;
    task_arg.key_addr = key_addr;
}
WriteRead::~WriteRead()
{
    pthread_mutex_unlock(&task_lock);
    pthread_mutex_unlock(&tasklisten_lock);
    pthread_mutex_destroy(&task_lock);
    pthread_mutex_destroy(&tasklisten_lock);
    pthread_rwlock_destroy(&rw_lock);
}
void WriteRead::WR_listen()
{
    listenfd = Socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(CACHESERV_IP);
    servaddr.sin_port = htons(CACHESERV_PORT);//test!!!!!!!!!!!!!
    Bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr));
    Listen(listenfd, listen_num);
    efd = Epoll_create(openmax);
    task_arg_listen.efd = efd;
    task_arg.efd = efd;
    tep.events = EPOLLIN | EPOLLET;
    tep.data.fd = listenfd;
    Epoll_ctl(efd, EPOLL_CTL_ADD, listenfd, &tep);
}
int WriteRead::WR_listenWait(int microsecond=500)
{
    int nready = Epoll_wait(efd, ep, openmax, microsecond);
    return nready;
}
void WriteRead::WR_listenHandler()
{
    pthread_mutex_lock(&tasklisten_lock);
    task_arg_listen.cliaddr = &cliaddr;
    task_arg_listen.listenfd = listenfd;
    task_arg_listen.num = ++num;
    Task TT(task_listen, &task_arg_listen);
    (*ThrPl).addTask(TT);            
    pthread_mutex_unlock(&tasklisten_lock);
}
void WriteRead::WR_cfdHandler(int index)
{
    pthread_mutex_lock(&task_lock);
    task_arg.buf = buf;
    task_arg.sockfd = ep[index].data.fd;
    Task TT(task_cfd, &task_arg);
    (*ThrPl).addTask(TT);
    pthread_mutex_unlock(&task_lock);
}
bool WriteRead::is_listenfd(int index)
{
    return ep[index].data.fd==listenfd;
}

#endif /* CACHESERVER_HPP */




