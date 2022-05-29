# 分布式缓存
实现基于C++的分布式缓存系统，设计了多Master、多Cache Server以及若干Client并存的分布式缓存架构，能够具有基础的数据读写、分布获取、状态上报功能，同时具备容灾能力。
## 功能
* 单个Cache server采用LRU机制保存指定数量的key，并主动淘汰多余的key，同时使用基于epoll和线程池的架构高并发地进行数据收发。
* Cache server使用一致哈希算法实现负载均衡，并且会定时向Master发送心跳包以通知自身存活情况。
* Master检测到新的Cache server接入时，向所有其它服务器发送扩容指令和新的节点分布列表，各服务器接到指令后通过一致哈希算法将数据转移到新的Cache server。
* Master发送缩容指令和新的节点分布列表，接到指定的服务器通过一致哈希算法将数据转移到其它Cache server，最后自动关闭。
* Master检测到某个Cache server没有发送心跳包后，向其它节点发送节点更新指令和新的节点分布列表，其它服务器收到指令后将所有备份数据通过一致哈希算法转移到其各自新的对应Cache server上。
* Client能生成随机数据并写入Cache server，或者根据key向其对应的Cache server服务器请求value值，当请求超时时，向Master端请求最新的节点分布。Cache server收到数据后会将数据发送到备份Cache server上进行备份处理。
* 主Master向从Master定时发送心跳包，当从Master检测到主Master停止工作时，会主动通知所有Cache server节点连接到新的Master，接替工作。

## 学习博客
* [缓存淘汰策略-LRU](https://glyhust.github.io/2022/05/03/缓存淘汰策略-LRU/)
* [通信格式及辅助函数](https://glyhust.github.io/2022/05/05/通信格式及辅助函数/)
* [一致性哈希](https://glyhust.github.io/2022/05/06/一致性哈希/)
* [cache模块—数据读写](https://glyhust.github.io/2022/05/10/cache模块—数据读写)
* [cache模块—心跳包](https://glyhust.github.io/2022/05/12/cache模块—心跳包)
* [client模块](https://glyhust.github.io/2022/05/16/client模块/)
* [master模块](https://glyhust.github.io/2022/05/20/master模块/)

## 运行环境
* Linux
* C++