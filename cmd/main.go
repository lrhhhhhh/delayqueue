package main

import (
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkadelayqueue/delayqueue"
	"kafkadelayqueue/utils/topic"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func InitKafka(c *delayqueue.Config) {
	admin, err := kafka.NewAdminClient(c.ProducerConfig.ConfigMap())
	if err != nil {
		panic(err)
	}

	var topics []kafka.TopicSpecification
	for _, duration := range c.DelayQueue.DelayDuration {
		topics = append(topics, kafka.TopicSpecification{
			Topic:             fmt.Sprintf(c.DelayQueue.DelayTopicFormat, duration),
			NumPartitions:     c.DelayQueue.NumPartition,
			ReplicationFactor: c.DelayQueue.Replicas},
		)
	}

	if c.DelayQueue.Debug && c.DelayQueue.Clear {
		//topic.Delete(admin, topics)
		topic.Create(admin, topics)
	}

	time.Sleep(time.Second * 3)
	admin.Close()
}

func main() {
	flag.Parsed()

	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}

	InitKafka(c)

	// pprof
	go http.ListenAndServe(":18081", nil)

	dq, err := delayqueue.New(c)
	if err != nil {
		panic(err)
	}
	dq.Run(c.DelayQueue.Debug)

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
