## 原理
将delayqueue视为producer和consumer之间的中间件
- (1) 生产者将延迟消息投递到delayqueue;
- (2) delayqueue先将延迟消息投递到kafka，同时添加到时间轮中，延迟消息在时间轮中到期后将到kafka中拉取数据并消费，消费后即投递到目标队列中
- (3) 消费者到目标队列中拉取消息并消费

## 故障恢复
由于消息全部都放到了kafka，依赖kafka做故障恢复。  
宕机时，中间件 delayqueue 直接遍历一遍 kafka，把未消费的消息重新读取到时间轮中  
在这个过程中如果遇到过期的消息，立即进行投递，未过期的消息则放入到时间轮中

## 优化 


## 依赖
- [Kafka](https://github.com/confluentinc/confluent-kafka-go)




## kafka 安装
`make up`

## 运行example
```shell
make up                             # 启动kafka
go run example/delayqueue/main.go   # 先运行中间件delayqueue
go run example/consumer/main.go     # 再运行生产者
go run example/producer/main.go     # 最后运行消费者
```

## 性能：
单机下replica  
partition数量、consumer group中consumer数量 


## 设计
一个delayqueue cover 多个topic，每个topic cover一个分区区间[l, r]