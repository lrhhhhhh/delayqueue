package main

import (
	"delayqueue/delayqueue"
	"delayqueue/job"
	"delayqueue/log"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// 用来消费真实的topic信息
func main() {
	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}

	numConsumer := 1
	topic := "real-topic"

	// example/producer/main.go 中生产 10000个延迟消息，ID范围为[0, 10000]
	FromId := 1
	ToId := 10000
	resultChan := make(chan int, 100)

	for i := 0; i < numConsumer; i++ {
		tp := kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(i),
			Offset:    kafka.OffsetStored,
		}

		c.ConsumerConfig.GroupId = "real-consumer"

		go consume(c.ConsumerConfig.ConfigMap(), tp, resultChan)
	}

	// 检查延迟消息是否全部被处理
	go check(resultChan, FromId, ToId, 15*time.Second)

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-exit:
			if s == syscall.SIGHUP {
				break
			}
			return
		}
	}
}

func consume(c *kafka.ConfigMap, tp kafka.TopicPartition, resultChan chan<- int) {
	consumer, err := kafka.NewConsumer(c)
	if err != nil {
		panic(err)
	}

	if err = consumer.Assign([]kafka.TopicPartition{tp}); err != nil {
		panic(err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Error(err)
			continue
		}

		var j job.Job
		if err = json.Unmarshal(msg.Value, &j); err != nil {
			log.Error(err)
			continue
		}

		timeDiff := time.Now().UnixMilli() - j.ExecTimeMs
		if timeDiff >= 1 {
			log.Infof("job: %+v, TimeDiff: %d >= 0?, tp=%v", j, timeDiff, msg.TopicPartition)
		}

		if _, err = consumer.CommitMessage(msg); err != nil {
			log.Error(err)
		}

		resultChan <- j.Id
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
