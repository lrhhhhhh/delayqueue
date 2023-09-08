package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkadelayqueue/delayqueue"
	"kafkadelayqueue/job"
	"kafkadelayqueue/log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"
)

// 用来消费真实的topic信息
func main() {
	go http.ListenAndServe(":18080", nil)

	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}

	n := 4
	topic := "real-topic"
	var tp [][]delayqueue.TopicPartition
	for i := 0; i < n; i++ {
		tp = append(tp, []delayqueue.TopicPartition{{topic, i, i}})
	}

	// example/producer/main.go 中生产 10000个延迟消息，ID范围为[0, 10000]
	m := 10000
	resultChan := make(chan int, m)

	// 4个消费者，每个负责一个partition
	for i := 0; i < n; i++ {
		tp := []kafka.TopicPartition{
			{
				Topic:     &topic,
				Partition: int32(i),
				Offset:    kafka.OffsetStored,
			},
		}

		c.ConsumerConfig.GroupId = "real-consumer"
		consumerCfg := c.ConsumerConfig.ConfigMap()

		consumer, err := kafka.NewConsumer(consumerCfg)
		if err != nil {
			panic(err)
		}

		err = consumer.Assign(tp)
		if err != nil {
			panic(err)
		}

		go consume(consumer, resultChan)
	}

	// 检查延迟消息是否全部被处理
	go check(resultChan, 1, m, 15*time.Second)

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-exit:
			if s == syscall.SIGHUP {
				break
			}
			log.Info("Server Signal Done")
			return
		}
	}
}

func consume(consumer *kafka.Consumer, resultChan chan<- int) {
	ticker := time.Tick(time.Second)
	ch := make(chan struct{})
	// commit async
	go func() {
		for {
			select {
			case <-ch:
			case <-ticker:
				res, err := consumer.Commit()
				if err != nil {
					if err.Error() != "Local: No offset stored" {
						log.Error(err.Error())
					}
				} else {
					_ = res
					//fmt.Printf("consumer commit: %+v\n", res)
				}
			}
		}
	}()

	Cnt := 0
	batchCommitSize := 1000

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer ReadMessage Err:%v, msg(%v)\n", err, msg)
			continue // NOTE:
		}

		var j job.Job
		err = json.Unmarshal(msg.Value, &j)
		if err != nil {
			log.Error(err.Error())
			continue // NOTE:
		}

		resultChan <- j.Id

		timeDiff := time.Now().Unix() - j.ExecTime
		if timeDiff >= 1 {
			log.Infof("job: %+v, TimeDiff: %d >= 0? offset: %v partition: %v\n",
				j, time.Now().Unix()-j.ExecTime, msg.TopicPartition.Offset, msg.TopicPartition.Partition)
		}

		Cnt += 1
		if Cnt >= batchCommitSize {
			Cnt = 0
			ch <- struct{}{}
		}

		_, err = consumer.CommitMessage(msg)
		if err != nil {
			log.Error(err.Error())
		}
	}
}

// 等待waitTime时间间隔后检查ID在[FromId, ToId]范围内的所有延迟消息是否被正常消费
func check(resultChan chan int, FromId, ToId int, waitTime time.Duration) {
	mp := make(map[int]int)
	ticker := time.NewTicker(waitTime)
	for {
		select {
		case id := <-resultChan:
			mp[id] += 1
			ticker.Reset(waitTime)
		case <-ticker.C:
			isValid := true
			for i := FromId; i <= ToId; i++ {
				if v, ok := mp[i]; !ok || v != 1 {
					isValid = false
					break
				}
			}
			if isValid {
				log.Info("recv all message, done!")
			} else {
				log.Infof("fail, %d diff\n", ToId-FromId+1-len(mp))
			}
			mp = make(map[int]int)
			ticker.Stop()
		}
	}
}
