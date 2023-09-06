package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkadelayqueue/delayqueue"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	flag.Parsed()

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

	topics = append(topics, "order-cancel") // this topic is used for example
	topics = append(topics, "stock-deduct")

	//deleteTopic(admin, topics) // NOTE: be careful
	createTopic(admin, topics, numPartition, replica)

	fmt.Println("create topic done")
	time.Sleep(time.Second * 5)
	admin.Close()

	// pprof
	go http.ListenAndServe(":18081", nil)

	for _, tp := range c.DelayQueue.TopicPartition {
		for i := tp.L; i <= tp.R; i++ {
			go runDelayQueue([]delayqueue.TopicPartition{{tp.Topic, i, i}})
		}
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case <-exit:
			time.Sleep(time.Second)
			return
		}
	}
}

func runDelayQueue(topicPartition []delayqueue.TopicPartition) {
	c := delayqueue.NewKafkaDelayQueueConfig()
	c.DelayQueue.TopicPartition = topicPartition

	queue, err := delayqueue.NewKafkaDelayQueue(c)
	if err != nil {
		panic(err)
	}

	queue.Run(c.DelayQueue.Debug)
}

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
