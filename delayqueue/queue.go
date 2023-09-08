package delayqueue

import (
	"delayqueue/consumer"
	"delayqueue/job"
	"delayqueue/log"
	"delayqueue/producer"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
	"sync"
	"time"
)

type DelayQueue struct {
	producer             *producer.Producer
	consumer             *consumer.Consumer
	config               *Config
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
	for i := range c.TopicPartition {
		tp := c.TopicPartition[i]
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
		log.Errorf("consumer assign %+v fail: %w", topicPartition, err)
		return nil, err
	}

	dqp.Run(c.Debug)

	return &DelayQueue{
		producer:             dqp,
		consumer:             dqc,
		config:               c,
		locker:               new(sync.Mutex),
		pausedTopicPartition: make(map[kafka.TopicPartition]struct{}),
	}, nil
}

// Run 监听延迟队列，到期投递到真正的队列，未到期则暂停消费延迟队列，ticker到期后恢复消费
func (dq *DelayQueue) Run(debug bool) {
	cnt := 0 // todo: mod or uint
	commitSignal := make(chan struct{})

	log.Info("DelayQueue is running...")

	// 检查delayqueue所负责的所有元组(topic,partition)，提交
	go func() {
		resumeTicker := time.Tick(50 * time.Millisecond)
		commitTicker := time.Tick(time.Duration(dq.config.BatchCommitDuration) * time.Millisecond)
		for {
			select {
			case <-resumeTicker:
				dq.locker.Lock()
				for tp := range dq.pausedTopicPartition {
					if err := dq.consumer.Resume([]kafka.TopicPartition{tp}); err != nil {
						log.Errorf("consumer resume err: %+v, TopicPartition: (%+v)", err, tp)
					} else {
						delete(dq.pausedTopicPartition, tp)
					}
				}
				dq.locker.Unlock()

			case <-commitSignal:
			case <-commitTicker:
				_, err := dq.consumer.Commit()
				if err != nil {
					if err.Error() != "Local: No offset stored" {
						log.Error(err.Error())
					}
				}
			}
		}
	}()

	for {
		msg, err := dq.consumer.ReadMessage(-1)
		if err != nil {
			if !errors.Is(err, kafka.NewError(kafka.ErrTimedOut, "", false)) {
				log.Errorf("Consumer ReadMessage Err:%v, msg(%v)\n", err, msg)
			}
			continue
		}

		var j job.Job
		err = json.Unmarshal(msg.Value, &j)
		if err != nil {
			log.Errorf("unmarshal Err:%v, msg(%v)\n", err, msg)
			continue
		}
		if j.Topic == "" {
			log.Errorf("empty topic: job(%+v)\n", j)
			continue
		}

		if msg.Timestamp.After(time.Now()) {
			if err = dq.pause(msg.TopicPartition); err != nil {
				log.Errorf("Consumer PauseAndSeekTopicPartition Err:%v, jobId(%d), topicPartition(%+v)\n", err, j.Id, msg.TopicPartition)
			}
			log.Debugf("job not ready: %+v, exec_time: %v, time_diff: %v > 0 ?\n",
				j, time.Unix(j.ExecTime, 0), j.ExecTime-time.Now().Unix())
		} else {
			err = dq.producer.Send(j.Topic, time.Now(), []byte(strconv.Itoa(j.Id)), msg.Value)
			if err != nil {
				log.Errorf("job投递ready队列失败(%v), job(%+v)\n", err, j) // TODO:
			} else {
				cnt += 1
				if cnt >= dq.config.BatchCommitSize {
					cnt = 0
					commitSignal <- struct{}{}
				}
			}

			log.Debugf("job has ready: %+v, exec_time: %v, time_diff: %v <= 0?\n",
				j, time.Unix(j.ExecTime, 0), j.ExecTime-time.Now().Unix(),
			)
		}
	}
}

// pause 暂停消费并重置offset
func (dq *DelayQueue) pause(tp kafka.TopicPartition) error {
	err := dq.consumer.Pause([]kafka.TopicPartition{tp})
	if err != nil {
		return err
	}

	dq.locker.Lock()
	defer dq.locker.Unlock()
	dq.pausedTopicPartition[tp] = struct{}{}

	return dq.consumer.Seek(tp, 50)
}

func (dq *DelayQueue) Close() {
	dq.producer.Close()
	dq.consumer.Close()
}

func (dq *DelayQueue) RecoverTimeWheel() {

}

// Restart 故障或停机后重启
// 需要重读kafka中所有未消费的消息到时间轮子中
func (dq *DelayQueue) Restart() {
	dq.RecoverTimeWheel()
	dq.Run(false)
}
