#ifndef CLIENT_HPP
#define CLIENT_HPP

#include "../common/json.hpp"
#include "../common/con_hash.hpp"
#include "../common/wrap_fun.hpp"
#include "arpa/inet.h"
#include "signal.h"


#define KEY_VALUE_REQUEST 0        //client向cache请求数据
#define KEY_VALUE_RESPOND 1
#define HEART_BEAT 2
#define DISTRIBUTION_REQUEST 3     //client向master拉取节点分布
#define DISTRIBUTION_RESPOND 4     //master向client发送节点分布
#define CACHE_SHUNTDOWN 5
#define CACHE_JOIN 6
#define MASTER_IP "127.0.0.1:7000"
#define MASTER_IP_BACKUP "127.0.0.1:7001"
#define KEY_FILE_NAME "keydata.txt"
#define TIME_OUT 5

using json=nlohmann::json;

class Client
{
private:
    ConsistentHash hash;
    json Writejs(int type);
    json Writejs(string key);
    json ReadData(int& cfd);
    int ConnectServ(ipport_pair oneip,int& cfd);
    int WriteData(int& cfd,json js);
    void DistributionRequest();
    void CountTime();
    int kvwritecfd,kvreqcfd,mastercfd;
    struct sockaddr_in serv_addr;
    socklen_t serv_addr_len;
    time_t start,end;
    struct timeval timeout={3,0};
public:
    Client();
    Client(vector<string> iplist);
    ~Client(){}
    vector<string> alliplist;
    void keyValueWrite();
    string keyValueRequest(string key);
};

//根据type写json变量
json Client::Writejs(int type)
{
    json js,data;
    if(type==KEY_VALUE_RESPOND)
    {
        data["key"]=RandStr(20);
        data["value"]=RandStr(20);
        data["flag"]=true;
    }
    else if(type==DISTRIBUTION_REQUEST)
        data=json::object();
    js["type"]=type;
    js["data"]=data;
    return js;
}
//返回请求key的value的json变量
json Client::Writejs(string key)
{
    json js,data;
    data["flag"]=true;
    data["key"]=key;
    data["value"]="";
    js["type"]=KEY_VALUE_REQUEST;
    js["data"]=data;
    return js;
}
//向oneip中的地址建立连接，失败则重复四次
int Client::ConnectServ(ipport_pair oneip,int& cfd)
{
    int j=0;
    cfd=Socket(AF_INET,SOCK_STREAM,0);
    int ret=setsockopt(cfd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
    bzero(&serv_addr,sizeof(serv_addr));
    serv_addr.sin_family=AF_INET;
    inet_pton(AF_INET,(oneip.ip.c_str()),&serv_addr.sin_addr.s_addr);
    serv_addr.sin_port=htons(oneip.port);
    int n=connect(cfd,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
    if(n<0)
    {
        do
        {
            Close(cfd);
            cfd=Socket(AF_INET,SOCK_STREAM,0);
            ret=setsockopt(cfd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
            n=connect(cfd,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
            sleep(1);
            if(n>0)
                break;
            j++;
        }while(j<=3);
    }
    return n;
}
//向cfd中写入json文件
int Client::WriteData(int& cfd,json js)
{
    int nwrite;
    int j=0;
    string buf=js.dump();
    buf+="\0";
    nwrite=Write(cfd,(char*)buf.data(),buf.length()+1);
    return nwrite;
}
//读取cfd中的json文件
json Client::ReadData(int& cfd)
{
    char bufin[BUFSIZ];
    int j=0;
    Sleep_ms(100);
    int n=Read(cfd,bufin,BUFSIZ);
    if(n<=0)
    {
        return json::object();
    }
    if(!json::accept(bufin))
    {
        perr_exit("no json data");
    }
    json js=json::parse(bufin);
    return js;
}

void Client::DistributionRequest()
{
    int n,nwrite;
    ipport_pair masterip=get_ipport(MASTER_IP);
    json client_to_master=Writejs(DISTRIBUTION_REQUEST);

    do
    {
        n=ConnectServ(masterip,mastercfd);
        if(n!=0)
        {
            Close(mastercfd);
            masterip=get_ipport(MASTER_IP_BACKUP);
            n=ConnectServ(masterip,mastercfd);
            if(n!=0)
            {
                perr_exit("connection to master failed");
            }
        }
        nwrite=WriteData(mastercfd,client_to_master);

    }while(nwrite<0);

    json jsin=ReadData(mastercfd);
    if(jsin["type"]!=DISTRIBUTION_RESPOND)
        perr_exit("distribution request returns fault");

    alliplist=jsin["data"]["iplist"].get<vector<string>>();

    if(alliplist.empty())
        perr_exit("invalid distribution");
    hash.RefreshIPList(alliplist);
    time(&start);
    cout<<"distribution request once"<<endl;
    Close(mastercfd);
}


Client::Client()
{
    DistributionRequest();
}

Client::Client(vector<string> iplist)
{
    alliplist.swap(iplist);
    hash.RefreshIPList(alliplist);
    time(&start);
}

//计时，当超过指定时间就重新拉取cache分布
void Client::CountTime()
{
    time(&end);
    if(end<start)
    {
        perr_exit("time count error");
    }
    time_t interval=end-start;
    bool _isout=(interval>TIME_OUT)?true:false;
    if(_isout)
    {
        DistributionRequest();
    }
}

void Client::keyValueWrite()
{
    signal(SIGPIPE,SIG_IGN);
    ofstream outfile;
    outfile.open(KEY_FILE_NAME,ios::app);
    if(!outfile.is_open())
        perr_exit("open file failed");

    json js=Writejs(KEY_VALUE_RESPOND);
    string key=js["data"]["key"].get<string>();
    string value=js["data"]["value"].get<string>();
    CountTime();
    ipport_pair oneipbf=get_ipport(hash.GetServerIndex(key));

    int n=ConnectServ(oneipbf,kvwritecfd);
    while(n<0)
    {
        Close(kvwritecfd);
        sleep(1);
        DistributionRequest();

        oneipbf=get_ipport(hash.GetServerIndex(key));
        n=ConnectServ(oneipbf,kvwritecfd);
    }

    int nwrite=WriteData(kvwritecfd,js);
    if(nwrite<0)
        oneipbf.port=0;
    outfile<<key<<' '<<value<<' '<<oneipbf.port<<endl;

    while(1)
    {
        js=Writejs(KEY_VALUE_RESPOND);
        key=js["data"]["key"].get<string>();
        value=js["data"]["value"].get<string>();
        CountTime();
        ipport_pair oneip=get_ipport(hash.GetServerIndex(key));
        if(oneip.ip!=oneipbf.ip||oneip.port!=oneipbf.port)
        {
            Close(kvwritecfd);
            n=ConnectServ(oneip,kvwritecfd);
            while(n<0)
            {
                Close(kvwritecfd);
                sleep(1);
                DistributionRequest();
                oneip=get_ipport(hash.GetServerIndex(key));
                n=ConnectServ(oneip,kvwritecfd);
            }
        }
        nwrite=WriteData(kvwritecfd,js);
        if(nwrite<0)
        {
            oneipbf.port=0;
            sleep(1);
            continue;
        }
        oneipbf=oneip;
        outfile<<key<<' '<<value<<' '<<oneip.port<<endl;
        Sleep_ms(300);
    }
    Close(kvwritecfd);
    outfile.close();
}

string Client::keyValueRequest(const string key)
{
    json js=Writejs(key);
    json jsin;
    ipport_pair oneip;
    int j=0;
    while(1)
    {
        CountTime();
        oneip=get_ipport(hash.GetServerIndex(key));
        int n=ConnectServ(oneip,kvreqcfd);
        while(n<0)
        {
            Close(kvreqcfd);
            sleep(1);
            DistributionRequest();

            oneip=get_ipport(hash.GetServerIndex(key));
            n=ConnectServ(oneip,kvreqcfd);
        }
        int nwrite=WriteData(kvreqcfd,js);
        if(nwrite<0)
        {
            Close(kvreqcfd);
            DistributionRequest();
            continue;
        }
        jsin=ReadData(kvreqcfd);
        if(jsin.empty())
        {
            Close(kvreqcfd);
            perr_exit("connection with cache fails");
        }
        else if(jsin["data"]["flag"]==false)
        {
            Close(kvreqcfd);
            DistributionRequest();
            j++;
            if(j>2)
                perr_exit("Data lose");
            continue;
        }
        else if(jsin["type"]==KEY_VALUE_REQUEST&&jsin["data"]["key"]==key)
            break;
        else 
            perr_exit("key value request returns fault");
    }
    Close(kvreqcfd);
    string value=jsin["data"]["value"].get<string>();
    return value;
}

#endif