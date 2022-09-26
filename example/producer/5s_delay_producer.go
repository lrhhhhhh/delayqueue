package main

import (
	"fmt"
	"kafkadelayqueue/delayqueue"
	"time"
)

func main() {
	c := delayqueue.NewKafkaDelayQueueConfig()
	queue, err := delayqueue.NewKafkaProducer(c)
	if err != nil {
		panic(err)
	}

	debug := true
	queue.Run(debug)

	n := 100000
	delay := 5 // delay 5 seconds
	for jobId := 1; jobId <= n; jobId++ {
		err := queue.AddJob(jobId, delay, "lrh", "")
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("produce finish")

	time.Sleep(time.Second * 10)
}
