#include "cache_server.hpp"
#include "WriteRead.hpp"
#include "HeartBeat.hpp"

int main()
{
    auto key_addr = make_shared<ConsistentHash>(100);
    key_addr->RefreshIPList({curen_addr});
    auto ipport_list = make_shared<vector<string>>();
    auto LC = make_shared<LRUCache>(100);
    auto LC_BK = make_shared<LRUCache>(100);
    auto ThrPl = make_shared<ThreadPool>(10);

    pthread_t tid_heartbeat;
    struct heartbeat_struct heartbeat_arg;
    heartbeat_arg.ThrPl = ThrPl;
    heartbeat_arg.LC = LC;
    heartbeat_arg.LC_BK = LC_BK;
    heartbeat_arg.key_addr = key_addr;
    heartbeat_arg.ipport_list = ipport_list;
    Pthread_create(&tid_heartbeat, nullptr, heart_beat, &heartbeat_arg);  
    
    WriteRead WR_CACHE(ThrPl, LC, LC_BK, key_addr);
    WR_CACHE.WR_listen();
    for(;;)
    {
        int nready = WR_CACHE.WR_listenWait(500);
        if(nready<=0)
            continue;
        pthread_rwlock_rdlock(&shutdown_lock);
        for(int i=0;i<nready;i++)
        {
            if(WR_CACHE.is_listenfd(i))
                WR_CACHE.WR_listenHandler();
            else
                WR_CACHE.WR_cfdHandler(i);
        }
        pthread_rwlock_unlock(&shutdown_lock);
    }
    return 0;
}

