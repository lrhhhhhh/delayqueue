package delayqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
	"sync"
	"time"
)

type kafkaDelayQueue struct {
	producer             *kafkaProducer
	consumer             *kafkaConsumer
	batchCommitSize      int
	batchCommitDuration  int
	locker               *sync.Mutex // guardian for pausePartition
	pausedTopicPartition map[kafka.TopicPartition]struct{}
}

func NewKafkaDelayQueue(c *KafkaDelayQueueConfig) (*kafkaDelayQueue, error) {
	producer, err := NewKafkaProducer(c)
	if err != nil {
		return nil, fmt.Errorf("NewKafkaProducer error: %w", err)
	}

	consumerCfg := NewKafkaConsumerConfig(c)
	consumer, err := NewKafkaConsumer(consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("NewKafkaConsumer error: %w", err)
	}

	var topicPartition []kafka.TopicPartition
	for _, tp := range c.DelayQueue.TopicPartition {
		for i := tp.L; i <= tp.R; i++ {
			topicPartition = append(topicPartition, kafka.TopicPartition{
				Topic:     &tp.Topic,
				Partition: int32(i),
				Offset:    kafka.OffsetStored,
			})
		}
	}

	err = consumer.Assign(topicPartition)
	if err != nil {
		return nil, fmt.Errorf("consumer assign %+v fail: %w", topicPartition, err)
	}

	producer.Run(c.DelayQueue.Debug)

	return &kafkaDelayQueue{
		producer:             producer,
		consumer:             consumer,
		batchCommitSize:      c.DelayQueue.BatchCommitSize,
		batchCommitDuration:  c.DelayQueue.BatchCommitDuration,
		locker:               new(sync.Mutex),
		pausedTopicPartition: make(map[kafka.TopicPartition]struct{}),
	}, nil
}

// Run 监听延迟队列，到期投递到真正的队列，未到期则暂停消费延迟队列，ticker到期后恢复消费
func (k *kafkaDelayQueue) Run(debug bool) {
	cnt := 0
	commitSignal := make(chan struct{})

	go func() {
		resumeTicker := time.Tick(50 * time.Millisecond)
		commitTicker := time.Tick(time.Duration(k.batchCommitDuration) * time.Millisecond)
		for {
			select {
			case <-resumeTicker:
				k.locker.Lock()
				for tp := range k.pausedTopicPartition {
					if err := k.consumer.Resume([]kafka.TopicPartition{tp}); err != nil {
						fmt.Printf("consumer resume err: %+v, TopicPartition: (%+v)", err, tp)
					} else {
						delete(k.pausedTopicPartition, tp)
					}
				}
				k.locker.Unlock()

			case <-commitSignal:
			case <-commitTicker:
				res, err := k.consumer.Commit()
				if err != nil {
					if err.Error() != "Local: No offset stored" {
						fmt.Println(err)
					}
				} else {
					if debug {
						fmt.Printf("consumer commit: %+v\n", res)
					}
				}
			}
		}
	}()

	for {
		msg, err := k.consumer.ReadMessage(-1)
		if err != nil {
			if !errors.Is(err, kafka.NewError(kafka.ErrTimedOut, "", false)) {
				fmt.Printf("Consumer ReadMessage Err:%v, msg(%v)\n", err, msg)
			}
			continue
		}

		var job Job
		err = json.Unmarshal(msg.Value, &job)
		if err != nil {
			fmt.Printf("unmarshal Err:%v, msg(%v)\n", err, msg)
			continue
		}
		if job.Topic == "" {
			fmt.Printf("empty topic: job(%+v)\n", job)
			continue
		}

		if msg.Timestamp.After(time.Now()) {
			if err = k.pause(msg.TopicPartition); err != nil {
				fmt.Printf("Consumer PauseAndSeekTopicPartition Err:%v, jobId(%s), topicPartition(%+v)\n", err, job.Id, msg.TopicPartition)
			}
			if debug {
				fmt.Printf(
					"job not ready: %+v, exec_time: %v, time_diff: %v > 0 ?\n",
					job, time.Unix(job.ExecTime, 0), job.ExecTime-time.Now().Unix(),
				)
			}
		} else {
			err = k.producer.Send(job.Topic, time.Now(), []byte(strconv.Itoa(job.Id)), msg.Value)
			if err != nil {
				fmt.Printf("job投递ready队列失败(%v), job(%+v)\n", err, job) // TODO:
			} else {
				cnt += 1
				if cnt >= k.batchCommitSize {
					cnt = 0
					commitSignal <- struct{}{}
				}
			}

			if debug {
				fmt.Printf(
					"job has ready: %+v, exec_time: %v, time_diff: %v <= 0?\n",
					job, time.Unix(job.ExecTime, 0), job.ExecTime-time.Now().Unix(),
				)
			}
		}
	}
}

// pause 暂停消费并重置offset
func (k *kafkaDelayQueue) pause(tp kafka.TopicPartition) error {
	err := k.consumer.Pause([]kafka.TopicPartition{tp})
	if err != nil {
		return err
	}

	k.locker.Lock()
	defer k.locker.Unlock()
	k.pausedTopicPartition[tp] = struct{}{}

	return k.consumer.Seek(tp, 50)
}

func (k *kafkaDelayQueue) Close() {
	k.producer.Close()
	_ = k.consumer.Close()
}
