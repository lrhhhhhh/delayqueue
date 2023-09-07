package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkadelayqueue/delayqueue"
	"kafkadelayqueue/producer"
	"kafkadelayqueue/utils/topic"
)

// 投递延迟消息到延迟队列
func main() {
	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}

	realTopic := "real-topic"
	topics := []kafka.TopicSpecification{{
		Topic:             realTopic,
		NumPartitions:     4,
		ReplicationFactor: 1}}
	admin, err := kafka.NewAdminClient(c.ProducerConfig.ConfigMap())
	if err != nil {
		panic(err)
	}
	topic.Create(admin, topics)
	admin.Close()

	queue, err := producer.New(&c.ProducerConfig)
	if err != nil {
		panic(err)
	}
	debug := true
	queue.Run(debug)

	n := 10000
	delay := 5 // delay 5 seconds
	for jobId := 1; jobId <= n; jobId++ {
		err := queue.AddJob(jobId, delay, realTopic, "")
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("produce finish")
}
