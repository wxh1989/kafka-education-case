# Kafka 使用的场景
## 系统解耦
![解耦.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644199947648-157a06ed-b3c5-41d6-a889-d49124eb259c.png#clientId=u2f9e841e-f57c-4&crop=0&crop=0&crop=1&crop=1&from=ui&id=ufca27edc&margin=%5Bobject%20Object%5D&name=%E8%A7%A3%E8%80%A6.png&originHeight=287&originWidth=970&originalType=binary&ratio=1&rotation=0&showTitle=false&size=18150&status=done&style=none&taskId=ud7df75e9-8d1c-41a5-a847-73e51446a8d&title=)
​

## 实时数据分析
![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644288167377-a0419937-6ff7-4db3-99af-780e02e665a7.png#clientId=u38721536-eb27-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=211&id=ub7eb60ee&margin=%5Bobject%20Object%5D&name=image.png&originHeight=264&originWidth=1105&originalType=binary&ratio=1&rotation=0&showTitle=false&size=32143&status=done&style=none&taskId=u4312e703-6dd5-413f-a4e4-adb66aab71c&title=&width=884)


## 数据采集
![Kafka 数据存储.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644200657331-b5e38336-ced6-4809-a3d8-a6d46650b2cd.png#clientId=u2f9e841e-f57c-4&crop=0&crop=0&crop=1&crop=1&from=ui&id=u02c3ff24&margin=%5Bobject%20Object%5D&name=Kafka%20%E6%95%B0%E6%8D%AE%E5%AD%98%E5%82%A8.png&originHeight=282&originWidth=866&originalType=binary&ratio=1&rotation=0&showTitle=false&size=24618&status=done&style=none&taskId=u3198f200-1fc3-4645-88e8-d90726e4482&title=)


# Kafka 基础概念
## Broker kafka 节点
      broker就是处理消息的节点及在服务器 上启动的一个kafka服务
### broker 配置
在kafka 的安装路径下 bin目录中找到server.properties文件对内容进行修改
#### 基础配置
```shell
#集群中唯一值 0 ~ 255
broker.id=0
#端口号
port=9092
#服务地址 注意 ：这里规定的明确的地址 就不能在使用 127.0.0.1 或者 localhost
listeners = PLAINTEXT://192.168.0.99:9092
#数据存放地址
log.dirs=/usr/local/kafka_2.12-3.0.0/data/kafka-logs
#每个主题的默认分区数
num.partitions=1
#每个主题分区的默认复制系数，建议增大此值，提高可用性
offsets.topic.replication.factor=1
#用于介绍请求和向客户端响应的线程数 默认3
num.network.threads=3
#zookeeper 服务器地址
zookeeper.connect=localhost:2181
```
#### 优化配置
**提高broker的 开启和关闭速度**
此配置有3中情况可以被用到

1. 服务器正常启动，用于打开每个分区的日志数据
1. 服务器崩溃重启，用于检查和截断每个分区的日志数据
1. 服务器正常关闭，用于关闭日志片段

默认对于一个日志目录只有一个线程，对于包含大量分区的服务器 ，一旦崩溃恢复就会很慢，
此参数尽量调大
```shell
num.recovery.threads.per.data.dir=1
```
**日志片段优化**
日志片段的大小，超过设置的值时，将新建分段文件
优点：避免频繁的关闭和分配新的日志段文件，从而**降低磁盘写入的整体效率**
```shell
log.segment.bytes=1073741824
```
**控制消息大小**
**这个值对性能有显著的影响**

- 值越大那么kafka 就会用越多的线程 去处理 这个消息，同时对网络带宽的要求也增大
- 这个值 是值 消息压缩后的 大小，实际大小可以 大于 此值
```shell
#默认值是1048588 （1MB）
message.max.bytes=1048588
```


#### 数据保留配置
这3个参数都决定了 数据的过期时间，如何3个都设置 已最小的 为准
推荐使用 log.retention.ms
```shell
#已毫毛为单位
log.retention.ms
#已分钟为单位
log.retention.minutes
#默认是 168 小时 及 一周
log.retention.hours=168
```
**每个分区字节大小决定保留数据，分区越大 保留的数据量越大**
此参数要和 log.retention.ms 结合考虑
假设 log.retention.ms值是 86 400 000毫秒 （1天）
log.segment 设置成1073741824 （1GB）
此时 数据量 超过了 1GB ，说明当前数据量不到一天就达到了1GB，超出的部分将被删除
```shell
#默认是1073741824 ，1073741824字节=1GB
log.retention.bytes=1073741824
```


## Topic 主题
       相同类型的事件被归集到一个分类，这个分类即为主题，每个事件必须分配到主题上，
       可以理解非数据库上的一张表
## partition 分区
每个主题可以自定义分区数量，应为一个主题的消息 分成若干分区，所以无法保证 整个topic的消息顺序，但是分区内的 消息 是 有序的，先进先出
### 如何决定一个主题的分区数量

1. 分析主题每秒中的写入和读取最大数据量是多少
1. 分析消费者每秒可以处理多大的消息
1. 分区数量=主题每秒写入和读取的最大体积/消费者每秒处理消息体积
1. **分区数量不是越多越好**，分区越多占用的内存越大，首领选取的过程越慢，在均衡的效率也会降低
### 分区副本
#### 控制器
kafka的控制作用是，进行首领选举，保证kafka高可用
控制器的 推选过程

1. 所有broker上线都会向zookeeper创建contoller临时节点，速度最快第一个上线的broker将创建成功
1. 其他的broker监听 watch contoller
1. 当contoller的broker与zookeeper心跳停止，contoller将被删除，同时其他的broker会收到通知
1. 收到contoller删除的通知后，重复第一步，选出新的控制器节点

![Kafka ACK.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644472898154-c63ecc8d-df49-44b1-a331-2d4bfab281ac.png#clientId=u89633344-8114-4&crop=0&crop=0&crop=1&crop=1&from=ui&id=aWgqo&margin=%5Bobject%20Object%5D&name=Kafka%20ACK.png&originHeight=408&originWidth=1010&originalType=binary&ratio=1&rotation=0&showTitle=false&size=31430&status=done&style=none&taskId=uaa6b6af7-fa1a-4594-b524-dec0cf35d46&title=)

#### 消息复制
消息复制保证kafka的数据可靠性，不会应为某个broker挂掉，从而丢失数据
客户端的请求，只会向leader节点发送，leader 分区接收到新的请求，同时分区节点向leader节点请求新数据
如果leader节点挂掉，将由控制器 重新推选出新的 leader,数据最全的分区将成为新的首领，这样就能保证数据的可靠性。
![Kafka ACK.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644461729128-6b213a76-2be4-467c-99a2-d04d492323d1.png#clientId=u89633344-8114-4&crop=0&crop=0&crop=1&crop=1&from=ui&id=Ug7aY&margin=%5Bobject%20Object%5D&name=Kafka%20ACK.png&originHeight=370&originWidth=877&originalType=binary&ratio=1&rotation=0&showTitle=false&size=24967&status=done&style=none&taskId=u255a51fe-4cfa-4434-8e94-a38f5c8b666&title=)
#### 高水位HW
高水位线由完全同步数据的分区组成ISR
消息的可见性: 高水位线保证了消费者只能访问最可靠的数据，从而避免异常情况出现
ISR：ISR由leader分区以及所有于leader完全同步的Follower副本组成
Log End Offset（LEO）表示副本写入下一条消息的位移值,从HW到LEO之间的数据是未全部同步数据
![Kafka ACK.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644474600208-e12658ec-3ebb-48cc-853d-1da333465e8b.png#clientId=u89633344-8114-4&crop=0&crop=0&crop=1&crop=1&from=ui&id=k90xo&margin=%5Bobject%20Object%5D&name=Kafka%20ACK.png&originHeight=481&originWidth=1123&originalType=binary&ratio=1&rotation=0&showTitle=false&size=29170&status=done&style=none&taskId=u505f5412-6dd7-4aff-895c-0b2e46e07fd&title=)
### 分区的分段概念
kafak为了提高检索数据的速度，将分区分成多个数据文件和对应的索引文件，达到小范围的查询，从而提高速度
下图中 xxx.index 就是 索引文件 xx.log 就是真正的数据文件
![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644245431476-259ee702-e0b9-4570-b731-03a58a690b2a.png#clientId=u301e2b26-a546-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=56&id=Zgae1&margin=%5Bobject%20Object%5D&name=image.png&originHeight=70&originWidth=1391&originalType=binary&ratio=1&rotation=0&showTitle=false&size=42897&status=done&style=none&taskId=uaf432080-23b2-4760-ba21-6b897d303f5&title=&width=1112.8)
数据检索的过程如下图
![Kafka ACK (1).png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644284751694-d9586193-870d-420f-8037-87be1314acda.png#clientId=u0e73ea84-76ab-4&crop=0&crop=0&crop=1&crop=1&from=ui&id=uvawA&margin=%5Bobject%20Object%5D&name=Kafka%20ACK%20%281%29.png&originHeight=456&originWidth=1042&originalType=binary&ratio=1&rotation=0&showTitle=false&size=35330&status=done&style=none&taskId=u1a930aed-0241-479b-b62c-66d3a14fb53&title=)
## Producer 生产者

- 事件的创建者，生产者将事件数据信息写入 kafka 主题
### 同步发送
同步消息 producer 通过 **send** 发送消息 然后调用**get**等待消息 返回信息 才算结束，如果kafka发生异常，将会返回错误信息，可以设置重试参数，使客户端重试这发送消息 直到成功
优点：保证数据的可靠性
缺点：发送消息相对较慢，在没拿到返回值时会短暂的堵塞其他消息的处理
### 异步发送
异步消息，只要将消息发送出去就算完成，即使kafka出现异常，也不进行重试
优点：消息发送速度较快
缺点：数据可能会丢失
解决方案：在send发送消息时候 可以传入回调函数，**回调类实现 Callback 接口** ，当异常时进入回调，可以在回调里进行重试发送，或者更好的处理方式
### 生产者基础配置
```shell
#kafka 集群地址
bootstrap.servers=boroker服务ip地址:端口,address2
#键值的序列化方式  实现了 org.apache.kafka.common.serialization.Serializer 这个接口的类
key.serializer=org.apache.kafka.common.serialization.StringSerializer
#值的序列化方式 
value.serializer=xx
#可以任意填写内容，可以用来描述 数据的来源
client.id=任意描述
```
### 可用性配置
kafka 为了防止同一时间点，大量请求涌入把kafka 服务器冲死，在producer增加了缓冲区，在达到缓冲区大小进行统一发送
​


- 该参数用于设置生产者内存缓冲区大小
- **解决问题 **：如果应用程序发送消息的速度 超过 发送到服务器的速度。会导致生产者空间不足，这个时候send 或者 partitionFor 方法可能会阻塞 或者 报异常
```shell
buffer.memory
```

- 该参数在 consumer 调用 send 或 partitionFor时,  缓冲区满了或者无元数据,的堵塞时间，超过这个时间将扔出异常
```shell
max.block.ms 
```
### 可靠性（数据丢失优化）配置
此配置要根据 是追求**吞吐量**还是追求的** 数据的可靠性**进行抉择
```shell

# ALl 这种方式速度最慢，但可靠性最高 ，生产者会等待所有broker 副本都收到消息 才算发送成功。
# 0 生产者只要消息发送出去就算成功，有可能造成数据丢失
# 1 只要首领节点收到消息就算成功，此配置也会丢消息，在首领节点挂掉	
ack=all
```
此配置用于在某些非崩亏异常时（例如发生在均衡期间，kafka 短暂的连不上），可以**让生产者重试提交消息**，保证数据可以发送成功
```shell
#重试提交消息的次数 每次重试之间 默认间隔 100ms
retries=10
#每次重试的时间间隔 默认100ms
retry.backoff.ms=100
```
### 性能优化配置

- 多个记录被发送到同一个分区时，生产者将尝试将记录一起批处理成更少的请求
- 消息批次配置
- producer 可以等待 消息到达一定量的后 ,将消息发送到 kafka
- **优点：减少与服务器请求次数**
```shell
#以字节为单位 默认16384
batch.size=以字节为单位
```

- 批次数据压缩配置
- 将一批数据进行压缩发送，从而较少
- **优点：减少网络带宽的压力**
```shell
#snappay 是google的压缩算法 优点占用 cup 较少
#gzip 有点是 压缩比高，缺点 cup开销大 ，网络带宽低可以用这种方式
#lz4 基本比不上 gzip
compression.type=snappy gzip lz4
```

- 此参数规定producer 在发送批次消息之前等待多长时间，即使批次消息没有达到 批次大小，

如果到了这个时间也将会被发送出去

- 已毫秒为单位
- **缺点：有一定的延迟**
- **优点：吞吐量**
```shell
linger.ms=5
```

- 该参数指定生产者收到服务器响应之前可以发送多少个消息
- **缺点：值越高 占用内存越高，**
- **有点：吞吐量会得提升**
```shell
max.in.flight.requests.per.connection
```
### 保证写入顺序配置
在特殊情况下，业务需要保证 数据的 顺序会用到这种组合的配置，但是会影响吞吐量

- 在收到服务器响应之前只能发送1个请求，如果有1个请求报错了，producer就会重试 提交请求，其他的请求被堵塞，直到上一个请求发送成功，才继续发送 下一个
```shell
#设置成1
max.in.flight.requests.per.connection=1
```
## Consumer 消费者

- 事件的处理者，消费者 从主题中读取 数据
### ConsumerGroup 消费者群组

- 消费者隶属于一个消费组,消费组概念方便 消费者的横向扩展,
- 当消费者数量跟不上 消息产生的数量时,可以向群里里添加更多的消费者
- 群组里的消费者,消费主题里的部分分区数据
- 消费者群组内的 消费者数量 建议 小于或等于 分区数量,应为多余的消费者,不会做任何事情,这样资源就被浪费了
- ![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644301580644-90b6cb3c-4bed-4477-87ab-7f61ba0fa1ae.png#clientId=uff2cae32-25c1-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=461&id=u5aa004a7&margin=%5Bobject%20Object%5D&name=image.png&originHeight=576&originWidth=929&originalType=binary&ratio=1&rotation=0&showTitle=false&size=32043&status=done&style=none&taskId=u8a29cb40-1f23-4416-b268-ca3adbf1672&title=&width=743.2)
- 多个消费组之间数据读取是相互隔离的
- ![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644302269172-c56059b1-99e3-4168-b20a-ffbc63bace0e.png#clientId=uff2cae32-25c1-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=382&id=ufcdccc7b&margin=%5Bobject%20Object%5D&name=image.png&originHeight=478&originWidth=1150&originalType=binary&ratio=1&rotation=0&showTitle=false&size=38700&status=done&style=none&taskId=uc0035c88-7141-4530-9e88-6f8ead3d2f6&title=&width=920)
### 在均衡
在均衡指的是将分区相对平均的分配给 消费组中的 消费者
​

#### 触发在均衡的条件如下

- 分区数量的变化
- 新的消费者加入群组
- 消费者死亡，消费者无法在一定时间内维持和broker的心跳，就认为此消费者死亡

kafka 为了保证数据的可靠性，在均衡区间 消费者会短暂的无法读取消息
​

#### kafka的均衡策略有三种

- **Range **

将若干个连续的分区，分配给消费者
算式是：剩余分区总数/剩余消费者数
如果消费者订阅了 多个主题，此策略会在成 分区分配不均匀，
有的消费者 消费的数据过大，有的消费者消费的数据又太小

- **RoundRobin 默认策略**

逐个分区轮询分配给 消费者，
这种策略会将分区平均分配给每个消费者，但是当新的 消费者加入或者分区数变化的时候，所有分区都要从新分配一遍

- **Strategy**

在保证 已分区不变的情况下，将新的分区 分配给消费者
​

### offset 偏移量
偏移量记录 消费者 读取消息下标，偏移量的计算方式是最新
最新偏移量的计算方式是：消费者提交偏移量时，**最后一个消息的下标 + 1**
好处：记录偏移量会在系统崩溃恢复好或在均衡时，可以追溯到上一次消费的位置，从而继续在偏移量的基础上继续消费
偏移量的处理是 kafka 的消费者的核心
kafka 偏移量处理不当 会造成 数据的丢失 和 重复读取
#### 数据丢失场景
自动提交偏移量，因给系统崩溃或者在均衡等情况， 数据还没处理完，当消费者从新上线时候未处理的数据就将丢失
![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644305734095-be50c6d3-e02a-4095-9a81-e1a715ea5174.png#clientId=u6a0d7aee-ef0f-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=410&id=u22da0609&margin=%5Bobject%20Object%5D&name=image.png&originHeight=513&originWidth=1528&originalType=binary&ratio=1&rotation=0&showTitle=false&size=41712&status=done&style=none&taskId=u3d8c6efa-3264-4981-99a3-f4af159a2dc&title=&width=1222.4)
#### 数据重复读取场景
消费者 数据已经全部处理完成，但是便宜量未成功提交，下次在消费者将从旧位置开始消费消息
![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1644307151224-1750137f-728d-4286-8f13-e4c4f00a996c.png#clientId=u6a0d7aee-ef0f-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=433&id=u08e7bae3&margin=%5Bobject%20Object%5D&name=image.png&originHeight=541&originWidth=1350&originalType=binary&ratio=1&rotation=0&showTitle=false&size=40932&status=done&style=none&taskId=u45f33b31-9a7c-4f4a-b413-1c8542de576&title=&width=1080)
#### 在均衡监听器
2中情况可以出发均衡监听，在监听器中 处理偏移量保证 ，偏移量的准确性
​


1. 重新分配分区之后和消费者开始读取之前触发监听器
1. 在均衡开始之前，停止读取消息之后触发监听器



#### 解决数据丢失或者重复读取的解决思路


防止消息丢失：关闭自动提交，ack 设置成 all
​

防止数据重复消费：保证offset的提交 和 数据的处理在一个事务内，要成功都成功，失败都失败。
**方式一：数据库控制offset**

1. 在数据库中建立 一张表 存储 偏移量
1. 程序被事务控制
1. 在均衡监听器 中 重新分配分区之后和消费者开始读取之前触发监听器， 读取和设置分区的偏移量。并在在均衡之前 停止读取消息之后提交事务

**方式二：数据库对业务数据加唯一性索引**
**​**

**方式三: 使用zookeepr 或者 redis 的分布式锁 推荐**
### 消费者配置
#### 基本配置
```shell
#消费者组名称
group.id = xx
#用于描述处理消息的 来源消费者
client.id
#默认是true 自定提交，建议改成手动提交
enable.auto.commit=false

#此参数指定 新消费这组订阅主题的消费方式 
#消费者长时间未上线，偏移量记录失效的 处理方式
#默认从最新的偏移量开始消费
#earliest 从头开始消费
auto.offset.reset=latest
```


#### 消息返回设置
消费者向broker请求数据时
可用数据量小于此配置，那么broker会等到有足够的数据才返回给消费者
**设置该参数大小依据**

1. 没有很多可用数据，但是消费者的CPU使用率过高，那么久需要把值调整的比默认值大
1. 消费者过多也需要将值设置的比默认值大，降低broker的负载
```shell
#默认1字节
fetch.min.bytes=1
```
broker等待数据满足fetch.min.bytes参数的的时间
返回给消费者数据， 要么超过等待时间或者可用数据量达到最大值
```shell
#默认500毫秒
fetch.max.wait.ms=500
```
**消费者调用poll返回消息的数量**
根据消费者的处理能力进行相应的调整
```shell
#默认500条
max.poll.records=500
```
#### 心跳配置
broker通过此设置来判断消费者 多久不发心跳，则认为死亡
```shell
#默认（3 秒）
session.timeout.ms=3000
```
消费者调用poll方法发送心跳的间隔
```shell
#默认3秒 要和 session.timeout 保持一致
heartbeat.interval.ms=3000
```
#### 分区策略
使用逐条分配策略最佳，默认是此配置
```shell
#默认值 RoundRobin 策略，
partition.assignment.strategy=class org.apache.kafka.clients.consumer.RangeAssignor,class org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```
### 配置常量类源码
**ConsumerConfig** 此类是配置项的常量 集成自 **AbstractConfig**，每个常量都有 对应的说明字段_DOC_
![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1643809351636-ed623013-051b-4d3f-b43a-25f8ad247865.png#clientId=u76bbb0ae-aa20-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=539&id=u06123019&margin=%5Bobject%20Object%5D&name=image.png&originHeight=674&originWidth=1375&originalType=binary&ratio=1&rotation=0&showTitle=false&size=106314&status=done&style=none&taskId=u46095c40-397d-495c-ba89-f667efd1129&title=&width=1100)
## 消息序列化


### 自定义序列化
自定义序列化需要实现 Serializer  序列化 实现 Deserializer
序列化代码
```java
package com.example.kafka.serializer;

import com.example.kafka.dao.Customer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Customer data) {
        byte [] serializeName ;
        int stringSize;
        if(data == null){
            return  null;
        }else {
            if(data.getCustomerName() != null){
                serializeName = data.getCustomerName().getBytes(StandardCharsets.UTF_8);
                stringSize = serializeName.length;
            }else{
                serializeName = new byte[0];
                stringSize = 0;
            }
            //我们要定义一个buffer用来存储age的值，
            // age是int类型，需要4个字节。
            // 还要存储name的值的长度（后面会分析为什么要存name的值的长度），
            // 用int表示就够了，也是4个字节，name的值是字符串，长度就有值来决定
            // ，所以我们要创建的buffer的大小就是这些字节数加在一起：4+4+name的值的长度

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            //put()执行完之后，buffer中的position会指向当前存入元素的下一个位置
            buffer.putInt(data.getCustomerId());
            //由于在读取buffer中的name时需要定义一个字节数组来存储读取出来的数据，但是定义的这个数组的长度无法得到，
            // 所以只能在存name的时候也把name的长度存到buffer中
            buffer.putInt(stringSize);
            buffer.put(serializeName);
            return buffer.array();
        }
    }

    @Override
    public void close() {

    }
}

```
反序列化代码
```java
package com.example.kafka.serializer;

import com.example.kafka.dao.Customer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Customer deserialize(String topic, byte[] data) {
        int id;
        int nameLength;
        String name;
        try{
            if(data == null){
                return null;
            }
            if(data.length < 8){
                throw new SerializationException("Size of data received by IntegerDeserializer is shorter than expected...");
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);//wrap可以把字节数组包装成缓冲区ByteBuffer
            //get()从buffer中取出数据，每次取完之后position指向当前取出的元素的下一位，可以理解为按顺序依次读取
            id = buffer.getInt();
            nameLength = buffer.getInt();
            /*
             * 定义一个字节数组来存储字节数组类型的name，因为在读取字节数组数据的时候需要定义一个与读取的数组长度一致的数组，要想知道每个name的值的长度，就需要把这个长度存到buffer中，这样在读取的时候可以得到数组的长度方便定义数组
             */
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            name = new String(nameBytes, "UTF-8");
            return new Customer(id, name);
        }catch(Exception e){
            throw new SerializationException("error when deserializing..."+e);
        }

    }

    @Override
    public Customer deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }


    @Override
    public void close() {

    }
}

```
### Avro 序列化
### Confluent Schema Registry 使用
下载免费社区版本 confluent-community
#### 配置
配置文件在Confluent 安装目录下的**/etc/schema-registry**
```shell
# The address the socket server listens on.
#   FORMAT:
#     listeners = listener_name://host_name:port
#   EXAMPLE:
#     listeners = PLAINTEXT://your.host.name:9092
#confluent 访问地址
listeners=http://192.168.0.99:8888
#confluent 所在服务器地址
host.name=192.168.0.99
# Use this setting to specify the bootstrap servers for your Kafka cluster and it
# will be used both for selecting the leader schema registry instance and for storing the data for
# registered schemas.
#kafka 集群地址
kafkastore.bootstrap.servers=PLAINTEXT://192.168.0.99:9092,PLAINTEXT://192.168.0.99:9093,PLAINTEXT://192.168.0.99:9094

# The name of the topic to store schemas in
# vro 默认的主题
kafkastore.topic=_schemas

# If true, API requests that fail will include extra debugging information, including stack traces
debug=false
~                                     
```
#### 启动
进入安装目录的bin目录下
```shell
./schema-registry-start ../etc/schema-registry/schema-registry.properties 
```
#### 生产者 代码
```java
    /**
     * 使用 实体类发送 Avro 消息
     */
    @Test
    public void  sendAvroMessageByEntity(){

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        System.out.println(KafkaAvroSerializer.class.getName());
        //使用 avro 序列化类
        kafkaProperties.put("value.serializer",KafkaAvroSerializer.class.getName());
        kafkaProperties.put("bootstrap.servers","192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        //指定Confluent 访问地址
        kafkaProperties.put("schema.registry.url","http://192.168.0.99:8888");

        User user = User.newBuilder().setName("build-name-wxh").setFavoriteColor("buildColor").setFavoriteNumber(300).build();
        Producer<String, User> producer = new KafkaProducer<String, User>(kafkaProperties);

        ProducerRecord<String,User> record = new ProducerRecord<>("schema-registry","user",user);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println(metadata.topic());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

```
#### 消费者 代码
```shell
    /**
     * 客户端 反序列化 Avro
     */
    @Test
    public void deserializerByAvro(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.99:9092,192.168.0.99:9093,192.168.0.99:9094");
        props.put("group.id", "schema-test");
        props.put("key.deserializer", StringDeserializer.class.getName());
        //设置value的反序列化类为自定义序列化类全路径
        props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://192.168.0.99:8888");


        Consumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList("schema-registry"));

        while (true){
            ConsumerRecords<String ,User> records = consumer.poll(100);
            for(ConsumerRecord<String,User> consumerRecord : records){
                try {
                    GenericRecord user = consumerRecord.value();
                    List<Schema.Field> fields = user.getSchema().getFields();
                    Schema.Field field ;
                    for(int i = 0 ; i <fields.size(); i++){
                        field = fields.get(i);
                        System.out.println(field.name()+":"+user.get(field.name()));
                    }
                    System.out.println(user.toString());
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }

            }
        }


```
## 顺序消费
kafka 顺序消费 会牺/牲掉性能
### 生产者实现顺序发送

1. 将消息发送到同一分区，应为同一分区内的 消息是有序的
1. 或者主题内只设置一个分区
1. 使用send().get()同步发送
1. 设置此参数 max.in.flight.requests.per.connection=1
1. 设置ACK成 ALl 防止数据丢失



### 消费者实现幂等性消费
消费者知道防止重复消费即可
## 
# kafa 安装
## 官网下载地址
[https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)

- 不要下载 source download 启动会报错

![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1642662495842-e0a99d65-7609-43ef-ae0c-ca229ad542d9.png#clientId=u32d90ab5-2568-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=396&id=u1b4e385d&margin=%5Bobject%20Object%5D&name=image.png&originHeight=495&originWidth=1292&originalType=binary&ratio=1&rotation=0&showTitle=false&size=57173&status=done&style=none&taskId=ueefe28bc-194f-4136-831d-bc130664c35&title=&width=1033.6)

- 将kafka 上传至 服务/usr/local目录下
- 解压缩 tgz
```shell
tar -zxvf kafka.tgz
```
## 配置 kafka server.properties
```shell
#集群ID 唯一标识
broker.id=0
#向生产者和消费者暴露的 kakka 地址端口
listeners = PLAINTEXT://192.168.0.99:9092
#数据存放地址
log.dirs=/usr/local/kafka_2.12-3.0.0/data/kafka-logs
#zookeeper 地址
zookeeper.connect=localhost:2181

```
## 启动 kafka 服务
```shell
 ./kafka-server-start.sh -daemon ../config/server.properties
```
## 查看 broker 是否启动成功
在zookeeper 客户端中 执行
```shell
//查询 linux broker 进程号
1
//在zookeeper 中查询broker ID
ls /brokers/ids
```
能看到下图 broker id 说明 kafka 上线成功
![image.png](https://cdn.nlark.com/yuque/0/2022/png/789898/1642664394702-153b1a64-61c7-4e4f-a19b-f06b23ddb882.png#clientId=u821d1a45-c7b4-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=39&id=uf56ed513&margin=%5Bobject%20Object%5D&name=image.png&originHeight=49&originWidth=1184&originalType=binary&ratio=1&rotation=0&showTitle=false&size=4875&status=done&style=none&taskId=u16a58097-f782-44e9-b112-bc8fff95884&title=&width=947.2)
## 测试命令
进入到kafka 安装目录的 bin目录下执行以下操作
### 创建topic

- ./kafka-topics.sh --create 穿件主题的基本命令
- --partitions 1 给topic创建一个分区
- --replication-factor 1 给topic创建一个副本
- --bootstrap-server 192.168.0.99:9092 指定使用哪个broker 创建topic
    - 测试中如果在server.properties 中配置了 listeners 就不能使用localhost 必须用 指定的ip地址访问
- --topic 主题的名字
```shell
./kafka-topics.sh --create --partitions 1 \
--replication-factor 1 \
--topic quickstart-events \
--bootstrap-server 192.168.0.99:9092
```
### 查询 topic 信息
```shell
./kafka-topics.sh --describe --topic quickstart-events --bootstrap-server 192.168.0.99:9092
```
# Kafka java客户端调用
[https://github.com/wxh1989/kafka-education-case](https://github.com/wxh1989/kafka-education-case) 完整的案例
## 生产者
同步消息
异步消息
异步消息回调处理
​

## 消费者
# Spring Kafka 使用
# Kafka 集群配置


# Kafka 各种异常的处理
## The Cluster ID g3hVoMp1TiKEjrOhmtJy5g doesn't match stored

- 问题原因：此异常提示，说明集群环境重复启动导致的，
- 处理方式：进入数据文件夹中 删除 mate.propeities 文件 然后重新启动集群环境
## Topic already has N partitions

- 问题原因：修改主题分区的数量 等于 现在的分区数量
- 处理方式：调大修改分区的数量
## llegalStateException: Subscription to topics, partitions and pattern are mutually exclusive

- 问题原因: 使用指定offset获取消息 （consumer.assign consumer.seek）  就不能使用主题订阅   consumer.subscribe
- 处理方式：这种获取消息的模式 ，选择适合业务的是使用
