#ifndef THREADPOOL
#define THREADPOOL

#include "stdio.h"
#include "stdlib.h"
#include "sys/types.h"
#include "sys/socket.h"
#include "cerrno"
#include "unistd.h"
#include "string.h"
#include "sys/epoll.h"
#include "fcntl.h"
#include "sys/time.h"
#include <fstream>
#include <iostream>
#include <vector>
#include "pthread.h"
#include <queue>

using callback=void(*)(void*);
using namespace std;

//任务结构体，将函数体和传入参数封装在一起
struct Task
{
    callback function;
    void* arg;
    Task()
    {
        function=nullptr;
        arg=nullptr;
    }
    Task(callback f,void* arg)
    {
        function=f;
        this->arg=arg;
    }
};

//任务队列，内部存放需要执行的函数
class TaskQueue
{
private:
    pthread_mutex_t m_mutex;
    queue<Task> m_queue;
public:
    TaskQueue()
    {
        pthread_mutex_init(&m_mutex,NULL);
    }
    ~TaskQueue()
    {
        pthread_mutex_destroy(&m_mutex);
    }
    //添加任务
    void addTask(Task& task)
    {
        pthread_mutex_lock(&m_mutex);
        m_queue.push(task);
        pthread_mutex_unlock(&m_mutex);
    }
    void addTask(callback func,void* arg)
    {
        pthread_mutex_lock(&m_mutex);
        Task task;
        task.function=func;
        task.arg=arg;
        m_queue.push(task);
        pthread_mutex_unlock(&m_mutex);
    }
    //取出任务
    Task takeTask()
    {
        Task t;
        pthread_mutex_lock(&m_mutex);
        if(m_queue.size()>0)
        {
            t=m_queue.front();
            m_queue.pop();
        }
        pthread_mutex_unlock(&m_mutex);
        return t;
    }
    inline int taskNumber()
    {
        return m_queue.size();
    }
};

//线程池类
class ThreadPool
{
private:
    pthread_mutex_t m_lock;
    pthread_cond_t m_notEmpty;
    pthread_t* m_threadIDs;
    TaskQueue* m_taskQ;
    int m_num;
    bool m_shutdown=false;

    static void* worker(void* arg);
    void threadExit();
public:
    ThreadPool(int num);
    ~ThreadPool();

    void addTask(Task task);
};
//创建min个线程执行work函数，再创建一个线程执行manager函数
ThreadPool::ThreadPool(int num)
{
    m_taskQ=new TaskQueue;
    do
    {
        m_num=num;
        m_threadIDs=new pthread_t[m_num];

        if(m_threadIDs==nullptr)
            break;

        memset(m_threadIDs,0,sizeof(pthread_t)*m_num);

        if(pthread_mutex_init(&m_lock,NULL)!=0||pthread_cond_init(&m_notEmpty,NULL)!=0)
            break;

        for(int i=0;i<m_num;i++)
        {
            int n=pthread_create(&m_threadIDs[i],NULL,worker,this);
            if(n==0)
                return;
            errno=n;
            perror("pthread_create error");
            exit(-1);
        }
    }while(0);
}
ThreadPool::~ThreadPool()
{
    m_shutdown=1;

    for(int i=0;i<m_num;i++)
    {
        pthread_cond_signal(&m_notEmpty);
    }

    if(m_taskQ) delete m_taskQ;
    if(m_threadIDs) delete[] m_threadIDs;
    pthread_mutex_destroy(&m_lock);
    pthread_cond_destroy(&m_notEmpty);
}
void ThreadPool::addTask(Task task)
{
    if(m_shutdown)
        return;
    m_taskQ->addTask(task);
    pthread_cond_signal(&m_notEmpty);
}

//退出当前进程
void ThreadPool::threadExit()
{
    pthread_t tid=pthread_self();
    for(int i=0;i<m_num;i++)
    {
        if(m_threadIDs[i]==tid)
        {
            m_threadIDs[i]=0;
            break;
        }
    }
    pthread_exit(NULL);
}
void* ThreadPool::worker(void* arg)
{
    ThreadPool* pool=static_cast<ThreadPool*>(arg);
    while(true)
    {
        pthread_mutex_lock(&pool->m_lock);
        //任务队列为空时
        while(pool->m_taskQ->taskNumber()==0&&!pool->m_shutdown)
        {
            //等待notEmpty信号量唤醒
            pthread_cond_wait(&pool->m_notEmpty,&pool->m_lock);
        }
        if(pool->m_shutdown)
        {
            pthread_mutex_unlock(&pool->m_lock);
            pool->threadExit();
        }

        //从任务队列中取出任务执行
        Task task=pool->m_taskQ->takeTask();
        pthread_mutex_unlock(&pool->m_lock);
        task.function(task.arg);
        task.arg=nullptr;
    }
    return nullptr;
}
#endif