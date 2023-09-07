package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkadelayqueue/delayqueue"
	"kafkadelayqueue/job"
	"log"
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

	n := 4
	topic := "delay-5s"
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
	c, err := delayqueue.LoadConfig()
	if err != nil {
		panic(err)
	}

	consumerCfg := c.ConsumerConfig.ConfigMap()
	GroupId := "real-consumer"
	if err := consumerCfg.SetKey("group.id", GroupId); err != nil {
		panic(err)
	}

	consumer, err := kafka.NewConsumer(consumerCfg)
	if err != nil {
		panic(err)
	}

	var t []kafka.TopicPartition
	for i := range topicPartition {
		tp := topicPartition[i]
		for partition := tp.L; partition <= tp.R; partition++ {
			t = append(t, kafka.TopicPartition{
				Topic:     &tp.Topic,
				Partition: int32(partition),
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

		var j job.Job
		err = json.Unmarshal(msg.Value, &j)
		if err != nil {
			fmt.Println(err)
			continue // NOTE:
		}

		resultChan <- j.Id

		timeDiff := time.Now().Unix() - j.ExecTime
		if timeDiff >= 1 {
			log.Printf(
				"ConsumerGroup: %v goroutine: %v job: %+v, TimeDiff: %d >= 0? offset: %v partition: %v\n",
				GroupId, gid, j, time.Now().Unix()-j.ExecTime, msg.TopicPartition.Offset, msg.TopicPartition.Partition)
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
