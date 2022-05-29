#ifndef MASTER_BASICALHPP
#define MASTER_BASICALHPP

#include "../common/wrap_fun.hpp"
#include "../common/threadpool.hpp"
#include "../common/json.hpp"
#include "arpa/inet.h"
#include "signal.h"

#define MASTER_IP "127.0.0.1"
#define MASTER_PORT 7001

#define HEART_BEAT 2
#define DISTRIBUTION_REQUEST 3
#define DISTRIBUTION_RESPOND 4
#define SHUTDOWN_CACHE 6
#define ADD_CACHE 7
#define SPARE_MASTER 8
#define REFLESH_MASTER 9
#define REFLESH_IP 10

pthread_rwlock_t rw_lock=PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t task_lock=PTHREAD_MUTEX_INITIALIZER;

vector<string> IPportlist;

using json=nlohmann::json;

vector<string> shutipnow,shutportnow;

//存放缩容时需要关闭的地址
string shutip,shutport;

string heartip,heartport;

unordered_map<string,int> cachestatemap;    //存储每个port的心跳包计数

unordered_map<string,int> precachestatemap; //存储前一时间的心跳包计数


//存储新加入的cache的地址
vector<string> IPlist;
vector<int> Portlist;

unordered_map<string,int> cachemap;  //发布更新后的节点信息，记录发送状态

unordered_map<string,int> cachemapshut;  //缩容时向其它cache发消息

unordered_map<string,int> masterupdatemap;

unordered_map<string,int> masterstatemap;     //存储主master的心跳包数量
unordered_map<string,int> premasterstatemap;  //新开线程，检测portlist里面每个master对应的心跳包计数是否一直在加
string masterip,masterport;  //提取出发送心跳包的master地址

string psshutip,psshutport;

int expan_flag=0;  //扩容标志位
int cache_shutdown=0;  //主动缩容标志位
int allcache_shutdown_down=0;  //关闭指令发完后，标志位置1
int cache_shutdown_done=0;  //防止已关闭的cache发生重连现象
int cache_shutdown_type=0;  //操作人员关闭多个cache，设置cache_shutdown=1，cache_shutdown_type=1，cache主动关闭自己时，设置cache_shutdown=1，cache_shutdown_type=2
int passive_shutdown_flag=0;  //被动缩容标志位，与进入主动缩容的函数区分开
int master_recovery=0;  //master容灾标志位，容灾完成后置1
int flag8=0;


int cache_flesh_count=0;  //发送计数
int num2=0;  //缩容时向其它cache发送计数
int cache_shutdown_count=0;  //向待关闭的cache发送计数
int shut_cache_size=0;  //主动缩容时存放待关闭cahce信息的shutportnow的长度
int reconnect_count=0;  //伪重连计数
int reconnect_maxnum=0;

void Distributionresquest(int clie_fd)
{
    pthread_rwlock_rdlock(&rw_lock);
    json data,listip;
    vector<string> dataip;
    int size=IPportlist.size();
    listip["type"]=DISTRIBUTION_RESPOND;
    for(int i=0;i<size;i++)
    {
        dataip.push_back(IPportlist[i]);
    }
    data["iplist"]=dataip;
    listip["data"]=data;
    string str_out=listip.dump();
    str_out+="\0";
    Write(clie_fd,(char*)str_out.data(),str_out.length()+1);
    pthread_rwlock_unlock(&rw_lock);
}

void delayms(const int ms)
{
    struct timeval delay;
    delay.tv_sec=ms/1000;
    delay.tv_usec=(ms%1000)*1000;
    select(0,NULL,NULL,NULL,&delay);
}

void refreship(int clie_fd)
{
    pthread_rwlock_rdlock(&rw_lock);
    json sum,data;

    if(cache_shutdown_done==1)
        sum["type"]=REFLESH_IP;
    else if(expan_flag==1)
        sum["type"]=ADD_CACHE;
    else if(cache_shutdown==1)
        sum["type"]=SHUTDOWN_CACHE;
    else if(allcache_shutdown_down==1)
        sum["type"]=REFLESH_IP;
    
    data["iplist"]=IPportlist;
    sum["data"]=data;
    string _buf=sum.dump();
    _buf+="\0";
    delayms(500);
    Write(clie_fd,(char*)_buf.data(),_buf.length());
    pthread_rwlock_unlock(&rw_lock);
}

void* masterhandler(void*)
{
    int openmax=1000;
    int num=0;
    char buf[BUFSIZ],str[openmax];
    struct sockaddr_in saddr,caddr;
    struct epoll_event tep,ep[openmax];
    int listen_num=10;

    //建立socket连接，用epoll函数监听
    int server_fd=Socket(AF_INET,SOCK_STREAM,0);

    int opt=1;
    setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    bzero(&saddr,sizeof(saddr));
    saddr.sin_family=AF_INET;
    saddr.sin_port=htons(MASTER_PORT);
    saddr.sin_addr.s_addr=inet_addr(MASTER_IP);
    int ret=Bind(server_fd,(struct sockaddr*)&saddr,sizeof(saddr));

    ret=Listen(server_fd,listen_num);

    int efd=Epoll_create(openmax);

    tep.events=EPOLLIN|EPOLLET;
    tep.data.fd=server_fd;
    int res=Epoll_ctl(efd,EPOLL_CTL_ADD,server_fd,&tep);
    cout<<"等待客户端连接..."<<endl;

    for(;;)
    {
        int nready=Epoll_wait(efd,ep,openmax,-1);

        for(int i=0;i<nready;i++)
        {
            //有新连接
            if(ep[i].data.fd==server_fd)
            {
                socklen_t clilen=sizeof(caddr);
                int client_fd=Accept(server_fd,(struct sockaddr*)&caddr,&clilen);
                cout<<"received from "<<inet_ntop(AF_INET,&caddr.sin_addr,str,sizeof(str))<<" at Port "<<ntohs(caddr.sin_port)<<endl;
                cout<<"cfd "<<client_fd<<"---client "<<++num<<endl;

                cout<<"对端的IP为 "<<inet_ntoa(caddr.sin_addr)<<endl;
                cout<<"对端的port为 "<<ntohs(caddr.sin_port)<<endl;

                auto flag=fcntl(client_fd,F_GETFL);
                fcntl(client_fd,F_SETFL,flag|O_NONBLOCK);

                tep.events=EPOLLIN|EPOLLOUT|EPOLLET;
                tep.data.fd=client_fd;

                res=Epoll_ctl(efd,EPOLL_CTL_ADD,client_fd,&tep);
            }
            else
            {
                int sockfd=ep[i].data.fd;
                ssize_t n=unblock_read_net(sockfd,buf,sizeof(buf));

                //没接到数据
                if(n==0)
                {
                    res=Epoll_ctl(efd,EPOLL_CTL_DEL,sockfd,NULL);
                    Close(sockfd);
                    cout<<"client["<<sockfd<<"] closed connection"<<endl;
                }
                else if(n<0)
                {
                    if(errno==EAGAIN)
                    {
                        cout<<"数据读完了..."<<endl;
                        break;
                    }
                    else
                    {
                        perr_exit("recv");
                        Close(sockfd);
                        exit(0);
                    }
                }
                else
                {
                    cout<<buf<<endl;
                    json info;
                    if(json::accept(buf))
                        info=json::parse(buf);
                    if(info["type"]==SPARE_MASTER&&master_recovery==0)
                    {
                        string masterdata=info["data"]["IP"];
						vector<string> vec=info["data"]["iplist"];
						if(vec.size()>0)
							IPportlist.assign(vec.begin(), vec.end());
						int cut = masterdata.find_last_of(':');
						masterip = masterdata.substr(0,cut);
						masterport = masterdata.substr(cut+1);
						auto it = masterstatemap.find(masterport);
						if(it != masterstatemap.end())
							(it->second)++;
						else
							masterstatemap.emplace(masterport,0);
                    }
                    else if(master_recovery==1)
                    {
                        if(info["type"]==DISTRIBUTION_REQUEST)
                            Distributionresquest(sockfd);
                        else if(info["type"]==SHUTDOWN_CACHE)
                        {
                            vector<string> userdata=info["data"]["iplist"];
                            for(int i=0;i<userdata.size();i++)
                            {
                                ipport_pair temp=get_ipport(userdata[i]);
                                shutipnow.push_back(temp.ip);
                                shutportnow.push_back(to_string(temp.port));
                            }
                            shut_cache_size=shutportnow.size();
                            cache_shutdown=1;
                            cache_shutdown_type=1;
                        }
                        else if(info["type"]==HEART_BEAT)
                        {
                            string heartdata=info["data"]["iplist"];
                            bool heartstate=info["data"]["state"];
                            ipport_pair temp=get_ipport(heartdata);
                            heartip=temp.ip;
                            heartport=to_string(temp.port);

                            auto it=cachestatemap.find(heartport);
                            if(it!=cachestatemap.end())
                                (it->second++);
                            
                            if(heartstate==false)
                                cache_shutdown_type=2;
                            
                            if(cache_shutdown_type==1)
                            {
                                auto it1=find(shutportnow.begin(),shutportnow.end(),heartport);
                                if(it1!=shutportnow.end())
                                    shutport=*it1;
                                auto it2=find(shutipnow.begin(),shutipnow.end(),heartip);
                                if(it2!=shutipnow.end())
                                    shutip=*it2;
                            }
                            else if(cache_shutdown_type==2)
                            {
                                shutport=heartport;
                                shutip=heartip;
                                cache_shutdown=1;
                            }
                            auto result=find(Portlist.begin(),Portlist.end(),atoi(heartport.c_str()));
                            if(result==Portlist.end())
                            {
                                if(reconnect_count<reconnect_maxnum)
                                {
                                    auto itt=find(shutportnow.begin(),shutportnow.end(),heartport);
                                    if(itt!=shutportnow.end())
                                        cache_shutdown_done=1;
                                    reconnect_count++;
                                }
                                else
                                {
                                    cache_shutdown_done=0;
                                    reconnect_count=0;
                                }
                                switch(cache_shutdown_done)
                                {
                                    case 0:
                                        IPlist.push_back(heartip);
                                        Portlist.push_back(atoi(heartport.c_str()));
                                        IPportlist.push_back(heartdata);
                                        cachemap.emplace(heartport,0);
                                        cachemapshut.emplace(heartport,0);
                                        cachestatemap.emplace(heartport,0);
                                        masterupdatemap.emplace(heartport,0);
                                        expan_flag=1;
                                        break;
                                    case 1:
                                        cache_shutdown_done=0;
                                        break;
                                    default:
                                        break;
                                }
                            }
                            if(expan_flag==1)
                            {
                                vector<string>().swap(shutportnow);
                                vector<string>().swap(shutipnow);

                                auto it=cachemap.find(heartport);
                                if(it!=cachemap.end())
                                {
                                    if(it->second==0)
                                    {
                                        it->second=1;
                                        cache_flesh_count++;
                                        refreship(sockfd);
                                    }
                                }
                                if(cache_flesh_count==cachemap.size())
                                {
                                    expan_flag=0;
                                    cache_flesh_count=0;
                                    for(auto& v:cachemap)
                                        v.second=0;
                                }
                            }
                            if(shutport==heartport&&cache_shutdown==1)
                            {
                                for(auto it=IPlist.begin();it!=IPlist.end();it++)
                                {
                                    if(*it==shutip)
                                    {
                                        IPlist.erase(it);
                                        break;
                                    }
                                }
                                for(auto it=Portlist.begin();it!=Portlist.end();it++)
                                {
                                    if(*it==atoi(shutport.c_str()))
                                    {
                                        Portlist.erase(it);
                                        break;
                                    }
                                }
                                for(auto it=IPportlist.begin();it!=IPportlist.end();it++)
                                {
                                    if(*it==heartdata)
                                    {
                                        IPportlist.erase(it);
                                        break;
                                    }
                                }
                                auto it1=cachemap.find(heartport);
                                if(it1!=cachemap.end())
                                    cachemap.erase(it1);
                                auto it2=cachemapshut.find(heartport);
                                if(it2!=cachemapshut.end())
                                    cachemapshut.erase(it2);
                                auto it3=cachestatemap.find(heartport);
                                if(it3!=cachestatemap.end())
                                    cachestatemap.erase(it3);
                                auto it4=precachestatemap.find(heartport);
                                if(it4!=precachestatemap.end())
                                    precachestatemap.erase(it4);
                                auto it5=masterupdatemap.find(heartport);
                                if(it5!=masterupdatemap.end())
                                    masterupdatemap.erase(it5);

                                refreship(sockfd);
                                cache_shutdown_count++;
                                flag8=1;
                                reconnect_maxnum=20;
                                shutport="";
                                shutip="";
                                if((cache_shutdown_type==1||cache_shutdown_type==2)&&(cache_shutdown_count==shut_cache_size&&flag8==1))
                                {
                                    cache_shutdown_count=0;
                                    cache_shutdown=0;
                                    allcache_shutdown_down=1;
                                    cache_shutdown_done=1;
                                    cache_shutdown_type=0;
                                    flag8=0;
                                }
                            }
                            else if(passive_shutdown_flag==1)
                            {
                                for(auto it=IPlist.begin();it!=IPlist.end();it++)
                                {
                                    if(*it==psshutip)
                                    {
                                        IPlist.erase(it);
                                        break;
                                    }
                                }
                                for(auto it=Portlist.begin();it!=Portlist.end();it++)
                                {
                                    if(*it==atoi(psshutport.c_str()))
                                    {
                                        Portlist.erase(it);
                                        break;
                                    }
                                }
                                string temp=psshutip+":"+psshutport;
                                for(auto it=IPportlist.begin();it!=IPportlist.end();it++)
                                {
                                    if(*it==temp)
                                    {
                                        IPportlist.erase(it);
                                        break;
                                    }
                                }
                                auto it1=cachemap.find(psshutport);
                                if(it1!=cachemap.end())
                                    cachemap.erase(it1);
                                auto it2=cachemapshut.find(psshutport);
                                if(it2!=cachemapshut.end())
                                    cachemapshut.erase(it2);
                                auto it3=cachestatemap.find(psshutport);
                                if(it3!=cachestatemap.end())
                                    cachestatemap.erase(it3);
                                auto it4=precachestatemap.find(psshutport);
                                if(it4!=precachestatemap.end())
                                    precachestatemap.erase(it4);
                                auto it5=masterupdatemap.find(psshutport);
                                if(it5!=masterupdatemap.end())
                                    masterupdatemap.erase(it5);

                                allcache_shutdown_down=1;
                                cache_shutdown_done=1;
                                reconnect_maxnum=20;
                                flag8=0;
                            }
                            if(allcache_shutdown_down==1&&flag8==0)
                            {
                                auto it=cachemapshut.find(heartport);
                                if(it!=cachemapshut.end())
                                {
                                    if(it->second==0)
                                    {
                                        it->second=1;
                                        num2++;
                                        refreship(sockfd);
                                    }
                                }
                                if(num2==cachemapshut.size())
                                {
                                    allcache_shutdown_down=0;
                                    passive_shutdown_flag=0;
                                    num2=0;
                                    for(auto& v:cachemapshut)
                                        v.second=0;
                                }
                            }
                        }
                    }
                }
            }
        }

    }
    Close(server_fd);
}

void* heartstate(void*)
{
    while(1)
    {
        while(master_recovery==1)
        {
            if(cachestatemap.size()>0)
            {
                precachestatemap=cachestatemap;
                delayms(1500);
                for(auto it=cachestatemap.begin();it!=cachestatemap.end();it++)
                {
                    auto itt=precachestatemap.find(it->first);
                    if(itt!=precachestatemap.end())
                    {
                        if(it->second<=itt->second)
                        {
                            psshutport=it->first;
                            psshutip=heartip;
                            if(passive_shutdown_flag==0)
                            {
                                if(psshutport!=shutport)
                                    passive_shutdown_flag=1;
                            }
                        }
                        else if(it->second>1000)
                            it->second=0;
                    }
                }
            }
        }
    }
}

json refreshmaster()
{
    json sum,data;
    data["iplist"]="127.0.0.1:7001";
    sum["type"]=REFLESH_MASTER;
    sum["data"]=data;
    return sum;
}

void *masterstate(void*)
{
    while(master_recovery==0)
    {
        if(masterstatemap.size()>0)
        {
            premasterstatemap=masterstatemap;
            delayms(1500);
            for(auto iter = masterstatemap.begin(); iter != masterstatemap.end(); iter++)
            {
                auto iterr = premasterstatemap.find(iter->first);
                if(iterr!=premasterstatemap.end())
                {
                    if(iter->second <= iterr->second)
                    {
                        for(int i=0;i<IPportlist.size();i++)
                        {
                            string IPnow=IPportlist[i].substr(0,9);
                            string Portnow=IPportlist[i].substr(10,4);
                            struct sockaddr_in serv_addr;
                            socklen_t serv_addr_len;
                            signal(SIGPIPE, SIG_IGN);
                            bzero(&serv_addr,sizeof(serv_addr));
                            serv_addr.sin_family = AF_INET;
                            inet_pton(AF_INET, IPnow.c_str(), &serv_addr.sin_addr.s_addr);
                            serv_addr.sin_port = htons(atoi(Portnow.c_str()));
                            int cfd = Socket_connect(true, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
                            int flags=fcntl(cfd,F_GETFL,0);
                            fcntl(cfd,F_SETFL,flags|O_NONBLOCK);
                            string buf1 = refreshmaster().dump();
                            buf1+="\0";
                            int n=0;
                            n = Write(cfd, (char *)buf1.data(), buf1.length()+1);
                            while(n<0)
                            {
                                cfd = Socket_connect(false, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
                                n = Write(cfd, (char *)buf1.data(), buf1.length()+1);
                            }
                            Close(cfd);
                        }
                        IPportlist.clear();
                        master_recovery=1;
                    }
                }
            }
        }
    }
}

#endif