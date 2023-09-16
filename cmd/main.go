package main

import (
	"delayqueue/delayqueue"
	"delayqueue/log"
	"delayqueue/utils/topic"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	for _, duration := range c.DelayDuration {
		topics = append(topics, kafka.TopicSpecification{
			Topic:             fmt.Sprintf(c.DelayTopicFormat, duration),
			NumPartitions:     c.NumPartition,
			ReplicationFactor: c.Replicas},
		)
	}

	if c.Debug && c.Clear {
		//topic.Delete(admin, topics)
		topic.Create(admin, topics)
	}

	time.Sleep(time.Second * 3)
	admin.Close()
}

func main() {
	flag.Parsed()

	// pprof
	go http.ListenAndServe(":18081", nil)

	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}

	if c.Debug {
		log.Init("DEBUG")
	}

	InitKafka(c)

	dq, err := delayqueue.New(c)
	if err != nil {
		panic(err)
	}

	dq.Run()

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
