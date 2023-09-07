package main

import (
	"kafkadelayqueue/delayqueue"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"
)

func main() {
	// use for pprof
	go http.ListenAndServe(":18081", nil)

	n := 8
	topic := "delay-5s"
	var tp [][]delayqueue.TopicPartition
	for i := 0; i < n; i++ {
		tp = append(tp, []delayqueue.TopicPartition{{topic, i, i}})
	}

	//n := 8
	//tp := [][]delayqueue.TopicPartition{
	//	[]delayqueue.TopicPartition{{"delay-5s", 0, 1}},
	//	[]delayqueue.TopicPartition{{"delay-5s", 2, 3}},
	//	[]delayqueue.TopicPartition{{"delay-5s", 4, 5}},
	//	[]delayqueue.TopicPartition{{"delay-5s", 6, 7}},
	//	[]delayqueue.TopicPartition{{"delay-5s", 8, 9}},
	//	[]delayqueue.TopicPartition{{"delay-5s", 10, 11}},
	//	[]delayqueue.TopicPartition{{"delay-5s", 12, 13}},
	//	[]delayqueue.TopicPartition{{"delay-5s", 14, 15}},
	//}

	for i := 0; i < n; i++ {
		go runDelayQueue(tp[i])
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
	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}
	c.DelayQueue.TopicPartition = topicPartition

	dq, err := delayqueue.New(c)
	if err != nil {
		panic(err)
	}

	dq.Run(c.DelayQueue.Debug)
}
