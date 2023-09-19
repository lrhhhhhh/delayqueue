## 原理
将delayqueue视为producer和consumer之间的中间件
- (1) 生产者将延迟消息投递到delayqueue;
- (2) delayqueue先将延迟消息投递到kafka delay topic（自定义的），同时添加到时间轮中，延迟消息在时间轮中到期后将其commit，投递到目标队列中
- (3) 消费者到目标队列中拉取消息并消费


## kafka 安装 (使用docker-compose)
```shell
cd delayqueue
make up   # 启动kafka
```

## 运行example
```shell
go run cmd/main.go                  # 运行中间件delayqueue
go run example/consumer/main.go     # 运行example中的生产者
go run example/producer/main.go     # 运行example中的消费者
```


## 可靠性 
从延迟队列中取出放入时间轮中等待，从时间轮中取出放入到目标队列，都可能存在提交失败的问题.  
但是只要保证发送后再提交+消息只消费一次（幂等性），可靠性还是可以保证的

## 故障恢复
由于消息全部都放到了kafka，依赖kafka做故障恢复。  
宕机时，中间件 delayqueue 从上一次提交开始读取，把未消费的消息重新读取到时间轮中  
在这个过程中如果遇到过期的消息，立即进行投递，未过期的消息则放入到时间轮中


## 性能：
性能并没有多好，时间轮应该运行在kafka中，而不是作为中间件存在

## TODO
- 更完备的测试


