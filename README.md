# MSIM

## pkg

### errors
错误集合


### keylock
管理锁


### mqtt
mqtt协议响应码定义和接口定义


### msdb
数据库管理，表定义


### mshttp

#### http
主要是对gin的封装和用pool管理

#### logger
根据logger返回调用函数


### mslog
封装zap等输出日志


### msnet
包含acceptor，自定义buffer，connect，event，listener等，还有engine之类的对于开源库的封装。同时对windows和linux类系统都做了支持

#### io
封装readv，writev

#### netpoll
const: pollevent的定义
基于epoll、kqueue、sys_epoll的epoll_event的调用，处理

#### socket
对不同系统以及不同协议的socket进行封装


### msutil
在 MSChatServerLib 库基础上增加

#### aes
增加了一个将加密数据写入 从pool中获取的bytebuffer中 的函数

#### arrays
数组的比较，插入，查找，删除

#### bitmap
位图的相关操作

#### channel
根据类型和id生成key，根据key解析

#### common
类型转换，进制转换等操作

#### convert
字符串转化为uint

#### data_pipeline
在协程中不断读取RingBuffer的数据

#### dh
生成密钥和公钥，根据密钥公钥生成key

#### encode
[]byte 和 interface{} 互转

#### fifo
先进先出的数据结构

#### file
和文件相关的操作

#### hash
通过字符串生成32位数字

#### ip
获取ip

#### json
格式转换

#### md5
md5加密

#### prase
类型转换

#### rate
分布式速率限制器。实现了内存防抖，gc。

#### ring_buffer
封装了一个RingBuffer，从pool中获取实例，进行各种操作

#### time
时间格式的转换

#### uuid
生成uuid

#### wai_group
对sync.WaitGroup的封装


### network
get等请求的封装


### pool

#### ringbuffer
对ring_buffer用池管理


### ring
实现ring_buffer的相关函数，如peek, read, write


### trace
封装了metric观察cluster，db 等值