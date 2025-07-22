
## MuShanIM JSON-RPC通信协议 


## 概述

jsonrpc:  可选，固定为"2.0"。如果省略，则假定为2.0。
method： 请求或通知的方法名
params： 请求或通知的参数
id: 请求的唯一标识符，响应必须包含与请求相同的id。
result： 成功响应的结果数据
error： 错误响应的错误对象

---
## 注意事项
1. 建立Websocket连接之后，需要在2s之内进行认证，超过2s和认证失败的连接会断开
2. 定期发送ping包

---
## 通用组件

### ErrorObject
请求处理失败时，响应中包含此对象
| 参数名   | 类型    | 是否必填 | 说明     |
| :------ | :----- | :----   | :------- |
| code    | integer| 是      | 错误码   |
| message | string | 是      | 错误描述 |
| data    | any    | 否      | 附加错误数据   |


### Header
可选的消息头信息
| 参数名  | 类型    | 是否必填 | 说明         |
| :------ | :--------- | :--------| :----------- |
| noPersist | boolean	 | 否       | 消息是否不存储     |
| redDot    | boolean  | 否       | 是否不显示红点    |
| syncOnce  | boolean  | 否       | 是否只被同步一次 |
| dup       | boolean	 | 否       | 是否是重发的消息  |


### SettingFlags
消息设置标记位
| 参数名    | 类型    | 是否必填 | 说明             |
| :-------- | :------ | :------- | :--------------- |
| receipt | boolean | 否       | 消息已读回执   |
| stream    | boolean | 否       | 是否为流式消息   |
| topic  | boolean | 否       | 是否包含topic |


---
## 消息类型
### 1.连接
#### Connect Request
第一个请求，用于建立连接和认证。
| 字段            | 类型    | 必填 | 描述                       |
| --------------- | ------- | ---- | -------------------------- |
| uid             | string  | 是   | 用户ID                     |
| token           | string  | 是   | 认证Token                  |
| header          | Header  | 否   | 消息头                     |
| version         | integer | 否   | 客户端协议版本             |
| clientKey       | string  | 否   | 客户端公钥                 |
| deviceId        | string  | 否   | 设备ID                     |
| deviceFlag      | integer | 否   | 设备标识 (1:APP, 2:WEB...) |
| clientTimestamp | integer | 否   | 客户端13位毫秒时间戳       |
##### 最小示例
```json
{
  "method": "connect",
  "params": {
    "uid": "testUser",
    "token": "testToken"
  },
  "id": "req-conn-1"
}
```
#### Connect Response
服务器对connect请求的响应。
##### 成功
| 字段          | 类型    | 必填 | 描述                       |
| ------------- | ------- | ---- | -------------------------- |
| serverKey     | string  | 是   | 服务端的DH公钥             |
| salt          | string  | 是   | 加密盐值                   |
| timeDiff      | integer | 是   | 客户端与服务器时间差(毫秒) |
| reasonCode    | integer | 是   | 原因码 (成功时通常为0)     |
| header        | Header  | 否   | 消息头                     |
| serverVersion | integer | 否   | 服务端版本                 |
| nodeId        | integer | 否   | 连接的节点ID               |
##### 失败
参考ErrorObject

##### 最小成功示例
```json
{
  "result": {
    "serverKey": "serverPublicKey",
    "salt": "randomSalt",
    "timeDiff": -15,
    "reasonCode": 0
  },
  "id": "req-conn-1"
}
```
##### 最小失败示例
```json
{
  "error": {
    "code": 1001,
    "message": "Authentication Failed"
  },
  "id": "req-conn-1"
}
```
### 2.发送
#### Send Request (send)
客户端发送消息到指定频道。

##### 参数 (params)
| 字段        | 类型         | 必填 | 描述                                    |
| ----------- | ------------ | ---- | --------------------------------------- |
| clientMsgNo | string       | 是   | 客户端消息唯一编号(UUID)                |
| channelId   | string       | 是   | 频道ID                                  |
| channelType | integer      | 是   | 频道类型 (1:个人, 2:群组)               |
| payload     | object       | 是   | 消息内容 (业务自定义JSON对象)           |
| header      | Header       | 否   | 消息头                                  |
| setting     | SettingFlags | 否   | 消息设置                                |
| msgKey      | string       | 否   | 消息验证Key                             |
| expire      | integer      | 否   | 消息过期时间(秒), 0表示不过期           |
| streamNo    | string       | 否   | 流编号 (如果 setting.stream 为 true)    |
| topic       | string       | 否   | 消息 Topic (如果 setting.topic 为 true) |
最小示例

```json
{
  "method": "send",
  "params": {
    "clientMsgNo": "uuid-12345",
    "channelId": "targetUser",
    "channelType": 1,
    "payload": {"content": "Hello!","type":1}
  },
  "id": "req-send-1"
}
```
#### Send Response
服务器对 send 请求的响应，表示消息已收到并分配了服务端 ID。

##### 成功结果 (result)
| 字段       | 类型    | 必填 | 描述                   |
| ---------- | ------- | ---- | ---------------------- |
| messageId  | string  | 是   | 服务端消息ID           |
| messageSeq | integer | 是   | 服务端消息序列号       |
| reasonCode | integer | 是   | 原因码 (成功时通常为0) |
| header     | Header  | 否   | 消息头                 |
##### 错误结果 (error)
参考 ErrorObject。

##### 最小成功示例

```json
{
  "result": {
    "messageId": "serverMsgId1",
    "messageSeq": 1001,
    "reasonCode": 0
  },
  "id": "req-send-1"
}
```
#### 3. 收到消息 (Recv)
##### Recv Notification (recv)
服务器推送消息给客户端。

##### 参数 (params)

| 字段        | 类型         | 必填 | 描述                                    |
| ----------- | ------------ | ---- | --------------------------------------- |
| messageId   | string       | 是   | 服务端消息ID                            |
| messageSeq  | integer      | 是   | 服务端消息序列号                        |
| timestamp   | integer      | 是   | 服务端消息时间戳(秒)                    |
| channelId   | string       | 是   | 频道ID                                  |
| channelType | integer      | 是   | 频道类型                                |
| fromUid     | string       | 是   | 发送者UID                               |
| payload     | object       | 是   | 消息内容 (业务自定义JSON对象)           |
| header      | Header       | 否   | 消息头                                  |
| setting     | SettingFlags | 否   | 消息设置                                |
| msgKey      | string       | 否   | 消息验证Key                             |
| expire      | integer      | 否   | 消息过期时间(秒) (协议版本 >= 3)        |
| clientMsgNo | string       | 否   | 客户端消息唯一编号 (用于去重)           |
| streamNo    | string       | 否   | 流编号 (协议版本 >= 2)                  |
| streamId    | string       | 否   | 流序列号 (协议版本 >= 2)                |
| streamFlag  | integer      | 否   | 流标记 (0:Start, 1:Ing, 2:End)          |
| topic       | string       | 否   | 消息 Topic (如果 setting.topic 为 true) |
##### 最小示例

```json
{
  "method": "recv",
  "params": {
    "messageId": "serverMsgId2",
    "messageSeq": 50,
    "timestamp": 1678886400,
    "channelId": "senderUser",
    "channelType": 1,
    "fromUid": "senderUser",
    "payload": {"content": "How are you?","type":1}
  }
}
```

#### 4. 收到消息确认 (RecvAck)
##### RecvAck Request (recvack)
客户端确认收到某条消息。

##### 参数 (params)

| 字段       | 类型    | 必填 | 描述                     |
| ---------- | ------- | ---- | ------------------------ |
| messageId  | string  | 是   | 要确认的服务端消息ID     |
| messageSeq | integer | 是   | 要确认的服务端消息序列号 |
| header     | Header  | 否   | 消息头                   |
##### 最小示例

```json
{
  "method": "recvack",
  "params": {
    "messageId": "serverMsgId2",
    "messageSeq": 50
  },
  "id": "req-ack-1"
}
```
(注：recvack 没有特定的响应体，如果需要响应，服务器可能会返回一个空的成功 result 或错误)

#### 5. 心跳 (Ping/Pong)
##### Ping Request (ping)
客户端发送心跳以保持连接。

##### 参数 (params)
通常为 null 或空对象 {}。

##### 最小示例
```json
{
  "method": "ping",
  "id": "req-ping-1"
}
```
##### Pong Response
服务器对 ping 请求的响应。

##### 成功结果 (result)
通常为 null 或空对象 {}。 
##### 错误结果 (error)
参考 ErrorObject。

最小成功示例

```json
{
  "method": "pong"
}
```