package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	go http.ListenAndServe(":18080", nil)

	//n := 4
	//tp := [][]delayqueue.TopicPartition{
	//	[]delayqueue.TopicPartition{{"lrh", 0, 3}},
	//	[]delayqueue.TopicPartition{{"lrh", 4, 7}},
	//	[]delayqueue.TopicPartition{{"lrh", 8, 11}},
	//	[]delayqueue.TopicPartition{{"lrh", 12, 15}},
	//}

	n := 128
	topic := "lrh"
	var tp [][]delayqueue.TopicPartition
	for i := 0; i < n; i++ {
		tp = append(tp, []delayqueue.TopicPartition{{topic, i, i}})
	}

	var jobs []int
	resultChan := make(chan int, 10000)

	for i := 0; i < n; i++ {
		go consume(tp[i], i, resultChan)
	}

	// check 100000 message
	go func() {
		d := 30 * time.Second
		ticker := time.NewTicker(d)
		for {
			select {
			case id := <-resultChan:
				jobs = append(jobs, id)
				ticker.Reset(d)
			case <-ticker.C:
				isValid := true
				mp := make(map[int]int)
				for i := 0; i < len(jobs); i++ {
					mp[jobs[i]] += 1
				}
				for i := 1; i <= 100000; i++ {
					v, ok := mp[i]
					if !ok || v != 1 {
						isValid = false
						break
					}
				}
				if isValid {
					fmt.Println("check!")
				} else {
					fmt.Printf("fail, %d diff\n", 100000-len(jobs))
				}
				jobs = jobs[0:0]
				ticker.Stop()
			}
		}
	}()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case s := <-exit:
			if s == syscall.SIGHUP {
				break
			}
			fmt.Println("Server Signal Done")
			return
		}
	}
}

func consume(topicPartition []delayqueue.TopicPartition, gid int, resultChan chan<- int) {
	c := delayqueue.NewConfig()
	consumerCfg := .NewKafkaConsumerConfig(c)
	fmt.Println(consumerCfg)

	GroupId := "real-consumer"
	err := consumerCfg.SetKey("group.id", GroupId)
	if err != nil {
		panic(err)
	}

	consumer, err := kafka.NewConsumer(consumerCfg)
	if err != nil {
		panic(err)
	}

	var t []kafka.TopicPartition
	for _, tp := range topicPartition {
		for i := tp.L; i <= tp.R; i++ {
			t = append(t, kafka.TopicPartition{
				Topic:     &tp.Topic,
				Partition: int32(i),
				Offset:    kafka.OffsetStored,
			})
		}
	}
	err = consumer.Assign(t)
	if err != nil {
		panic(err)
	}

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
						fmt.Println(err)
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

		var job delayqueue.Job
		err = json.Unmarshal(msg.Value, &job)
		if err != nil {
			fmt.Println(err)
			continue // NOTE:
		}

		resultChan <- job.Id

		timeDiff := time.Now().Unix() - job.ExecTime
		if timeDiff >= 1 {
			fmt.Printf(
				"ConsumerGroup: %v goroutine: %v job: %+v, TimeDiff: %d >= 0? offset: %v partition: %v\n",
				GroupId, gid, job, time.Now().Unix()-job.ExecTime, msg.TopicPartition.Offset, msg.TopicPartition.Partition)
		}

		Cnt += 1
		if Cnt >= batchCommitSize {
			Cnt = 0
			ch <- struct{}{}
		}

		//_, err = consumer.CommitMessage(msg)
		//if err != nil {
		//	fmt.Println(err)
		//}
	}
}
