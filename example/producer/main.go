package main

import (
	"delayqueue/delayqueue"
	"delayqueue/job"
	"delayqueue/log"
	"delayqueue/producer"
	"delayqueue/utils/topic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

// 投递延迟消息到延迟队列
func main() {
	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}

	targetTopic := "real-topic"
	numPartition := 1
	replicas := 1

	admin, err := kafka.NewAdminClient(c.ProducerConfig.ConfigMap())
	if err != nil {
		panic(err)
	}
	topic.Create(admin, []kafka.TopicSpecification{{
		Topic:             targetTopic,
		NumPartitions:     numPartition,
		ReplicationFactor: replicas,
	}})
	admin.Close()

	queue, err := producer.New(&c.ProducerConfig)
	if err != nil {
		panic(err)
	}
	queue.Run(true)

	n := 100
	delayMs := 5000 // delay 5 seconds = 5000ms
	for jobId := 1; jobId <= n; jobId++ {
		if err := queue.AddJob(&job.Job{
			Id:         jobId,
			Topic:      targetTopic,
			Body:       "",
			DelayMs:    int64(delayMs),
			ExecTimeMs: int64(delayMs) + time.Now().UnixMilli(),
		}); err != nil {
			panic(err)
		}
	}
	log.Debug("produce finish")
	time.Sleep(time.Second * 30)
}
