#include "master_basical.hpp"

int main(void)
{

    pthread_t tid_master;
    Pthread_create(&tid_master,NULL,masterhandler,NULL);

    //新开线程，检测cache心跳包计数是否持续增加
    pthread_t tid_heartbeat;
    Pthread_create(&tid_heartbeat,NULL,heartstate,NULL);

    //向从master发送心跳包
    pthread_t tid_masterheartbeat;
    Pthread_create(&tid_masterheartbeat,NULL,masterupdata,NULL);

    pthread_join(tid_master,NULL);
    return 0;
}