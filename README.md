# MSIM

## pkg

### mslog
封装zap等输出日志

### msnet

#### io
封装readv，writev

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

#### ring_buffer
封装了一个RingBuffer，从pool中获取实例，进行各种操作

### pool

#### ringbuffer
对ring_buffer用池管理

### ring
实现ring_buffer的相关函数，如peek, read, write