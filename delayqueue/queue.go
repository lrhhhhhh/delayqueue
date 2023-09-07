package delayqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkadelayqueue/consumer"
	"kafkadelayqueue/job"
	"kafkadelayqueue/producer"
	"log"
	"strconv"
	"sync"
	"time"
)

type DelayQueue struct {
	producer             *producer.Producer
	consumer             *consumer.Consumer
	batchCommitSize      int
	batchCommitDuration  int
	locker               *sync.Mutex // guardian for pauseTopicPartition
	pausedTopicPartition map[kafka.TopicPartition]struct{}
}

func New(c *Config) (*DelayQueue, error) {
	// DelayQueueProducer -> dqp
	dqp, err := producer.New(&c.ProducerConfig)
	if err != nil {
		return nil, fmt.Errorf("NewKafkaProducer error: %w", err)
	}

	// DelayQueueConsumer -> dqc
	dqc, err := consumer.New(&c.ConsumerConfig)
	if err != nil {
		return nil, fmt.Errorf("NewKafkaConsumer error: %w", err)
	}

	var topicPartition []kafka.TopicPartition
	for i := range c.DelayQueue.TopicPartition {
		tp := c.DelayQueue.TopicPartition[i]
		for partition := tp.L; partition <= tp.R; partition++ {
			topicPartition = append(topicPartition, kafka.TopicPartition{
				Topic:     &tp.Topic,
				Partition: int32(partition),
				Offset:    kafka.OffsetStored,
			})
		}
	}

	err = dqc.Assign(topicPartition)
	if err != nil {
		return nil, fmt.Errorf("consumer assign %+v fail: %w", topicPartition, err)
	}

	dqp.Run(c.DelayQueue.Debug)

	return &DelayQueue{
		producer:             dqp,
		consumer:             dqc,
		batchCommitSize:      c.DelayQueue.BatchCommitSize,
		batchCommitDuration:  c.DelayQueue.BatchCommitDuration,
		locker:               new(sync.Mutex),
		pausedTopicPartition: make(map[kafka.TopicPartition]struct{}),
	}, nil
}

// Run 监听延迟队列，到期投递到真正的队列，未到期则暂停消费延迟队列，ticker到期后恢复消费
func (k *DelayQueue) Run(debug bool) {
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
						log.Printf("consumer resume err: %+v, TopicPartition: (%+v)", err, tp)
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
						log.Println(err)
					}
				} else {
					if debug {
						log.Printf("consumer commit: %+v\n", res)
					}
				}
			}
		}
	}()

	for {
		msg, err := k.consumer.ReadMessage(-1)
		if err != nil {
			if !errors.Is(err, kafka.NewError(kafka.ErrTimedOut, "", false)) {
				log.Printf("Consumer ReadMessage Err:%v, msg(%v)\n", err, msg)
			}
			continue
		}

		var j job.Job
		err = json.Unmarshal(msg.Value, &j)
		if err != nil {
			log.Printf("unmarshal Err:%v, msg(%v)\n", err, msg)
			continue
		}
		if j.Topic == "" {
			log.Printf("empty topic: job(%+v)\n", j)
			continue
		}

		if msg.Timestamp.After(time.Now()) {
			if err = k.pause(msg.TopicPartition); err != nil {
				log.Printf("Consumer PauseAndSeekTopicPartition Err:%v, jobId(%d), topicPartition(%+v)\n", err, j.Id, msg.TopicPartition)
			}
			if debug {
				log.Printf(
					"job not ready: %+v, exec_time: %v, time_diff: %v > 0 ?\n",
					j, time.Unix(j.ExecTime, 0), j.ExecTime-time.Now().Unix(),
				)
			}
		} else {
			err = k.producer.Send(j.Topic, time.Now(), []byte(strconv.Itoa(j.Id)), msg.Value)
			if err != nil {
				log.Printf("job投递ready队列失败(%v), job(%+v)\n", err, j) // TODO:
			} else {
				cnt += 1
				if cnt >= k.batchCommitSize {
					cnt = 0
					commitSignal <- struct{}{}
				}
			}

			if debug {
				log.Printf(
					"job has ready: %+v, exec_time: %v, time_diff: %v <= 0?\n",
					j, time.Unix(j.ExecTime, 0), j.ExecTime-time.Now().Unix(),
				)
			}
		}
	}
}

// pause 暂停消费并重置offset
func (k *DelayQueue) pause(tp kafka.TopicPartition) error {
	err := k.consumer.Pause([]kafka.TopicPartition{tp})
	if err != nil {
		return err
	}

	k.locker.Lock()
	defer k.locker.Unlock()
	k.pausedTopicPartition[tp] = struct{}{}

	return k.consumer.Seek(tp, 50)
}

func (k *DelayQueue) Close() {
	k.producer.Close()
	_ = k.consumer.Close()
}
