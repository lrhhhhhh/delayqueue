package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"

	"kafkadelayqueue/delayqueue"
)

func createTopic(admin *kafka.AdminClient, topics []string, numPartition, replica int) {
	for _, topic := range topics {
		results, err := admin.CreateTopics(
			context.Background(),
			[]kafka.TopicSpecification{{
				Topic:             topic,
				NumPartitions:     numPartition, // NOTE:
				ReplicationFactor: replica}},
			kafka.SetAdminOperationTimeout(time.Second))
		if err != nil {
			fmt.Printf("Failed to create topic: %v\n", err)
		}

		for _, result := range results {
			fmt.Printf("%s\n", result)
		}
	}
}

func deleteTopic(admin *kafka.AdminClient, topics []string) {
	results, err := admin.DeleteTopics(context.Background(), topics)
	if err != nil {
		panic(err)
	}
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}
}

func main() {
	c := delayqueue.NewKafkaDelayQueueConfig()
	kafkaCfg := delayqueue.NewKafkaProducerConfig(c)
	fmt.Printf("%+v\n", kafkaCfg)

	admin, err := kafka.NewAdminClient(kafkaCfg)
	if err != nil {
		panic(err)
	}

	numPartition := 128
	replica := 1
	var topics []string
	for _, duration := range c.DelayQueue.DelayDuration {
		topics = append(topics, fmt.Sprintf(c.DelayQueue.DelayTopicFormat, duration))
	}

	topics = append(topics, "lrh") // this topic is used for example

	//deleteTopic(admin, topics) // NOTE: be careful
	createTopic(admin, topics, numPartition, replica)
}
