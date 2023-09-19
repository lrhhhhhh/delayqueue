package delayqueue

import (
	"delayqueue/consumer"
	"delayqueue/job"
	"delayqueue/log"
	"delayqueue/producer"
	"delayqueue/utils"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lrhhhhhh/timewheel"
	"strconv"
	"sync"
	"time"
)

type DelayQueue struct {
	producer       *producer.Producer
	consumer       *consumer.Consumer
	tw             *timewheel.TimeWheel
	config         *Config
	locker         *sync.Mutex // guardian for pauseTopicPartition
	topicPartition []kafka.TopicPartition
}

func New(c *Config) (*DelayQueue, error) {
	// DelayQueueProducer -> dqp
	dqp, err := producer.New(&c.ProducerConfig)
	if err != nil {
		log.Error(err)
		return nil, fmt.Errorf("NewKafkaProducer error: %w", err)
	}

	// DelayQueueConsumer -> dqc
	dqc, err := consumer.New(c.ConsumerConfig)
	if err != nil {
		log.Error(err)
		return nil, fmt.Errorf("NewKafkaConsumer error: %w", err)
	}

	tw, err := timewheel.New(c.Step * int(time.Millisecond))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &DelayQueue{
		producer: dqp,
		consumer: dqc,
		tw:       tw,
		config:   c,
		locker:   new(sync.Mutex),
	}, nil
}

func (dq *DelayQueue) handleMessage(message kafka.Message) {
	var jb job.Job
	if err := json.Unmarshal(message.Value, &jb); err != nil {
		log.Error(err)
	}
	log.Infof("handle message=%v, diff=%v", jb, time.Millisecond*time.Duration(time.Now().UnixMilli()-jb.ExecTimeMs))

	if err := dq.consumer.UpdateOffset(&dq.consumer.LoTp, &message.TopicPartition); err != nil {
		panic(err)
	}

	if err := dq.producer.Send(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &jb.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(strconv.Itoa(jb.Id)),
		Value:          message.Value,
		Timestamp:      time.Now(),
	}); err != nil {
		log.Error(err)
		return
	}
	// todo: batch commit
	if _, err := dq.consumer.CommitMessage(&message); err != nil {
		log.Error(err)
	}
}

func (dq *DelayQueue) startTimeWheel() {
	var jb job.Job

	if err := dq.consumer.SeekHighOffset(); err != nil {
		log.Error(err)
		panic(err)
	}

	flag := true
	retry := 10
	for (flag && retry > 0) || dq.tw.Size() < dq.config.MaxTimeWheelSize {

		log.Info(dq.consumer.Debug(dq.config.Debug))

		message, err := dq.consumer.ReadMessage(time.Millisecond * 500)
		if err != nil {
			if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrTimedOut {
				log.Debugf("[InitTW] ReadTimeout, retry=%d", retry)
				retry -= 1
			} else {
				log.Error(err)
			}
			if retry == 0 {
				break
			} else {
				continue
			}
		}

		flag = false
		if err = json.Unmarshal(message.Value, &jb); err != nil {
			log.Error(err)
		}
		if jb.ExecTimeMs < time.Now().UnixMilli() {
			log.Debugf("[InitTW] Message timed out, send it immediately, msg=%+v, jb=%s", message, jb.String())
			if err := dq.handleTimeoutMessage(message, &jb); err != nil {
				log.Error(err)
			}
		} else {
			// 否则投递到时间轮中
			// 读取m个未超时的消息到时间轮中 （读取但不commit，这里用游标 twOffset标记）
			//（另外，设对应的最后一个为commit的消息的游标为Offset）
			log.Debugf("[InitTW] Put into TW, msg=%+v", message)
			if err := dq.tw.Put(&timewheel.Event{
				Key:      utils.Tp2Key(&message.TopicPartition), // 记录key=topic-partition-offset是否在时间轮中
				Interval: int(jb.DelayMs) * int(time.Millisecond),
				Cnt:      1,
				RunSync:  true,
				Callback: func() { dq.handleMessage(*message) },
			}); err != nil {
				log.Error(err)
			}
		}
		if err := dq.consumer.UpdateOffset(&dq.consumer.HiTp, &message.TopicPartition); err != nil {
			log.Error(err)
		}
	}
	go dq.tw.Run()
	log.Info("[InitTW] done")
}

func (dq *DelayQueue) handleTimeoutMessage(message *kafka.Message, jb *job.Job) error {
	if err := dq.producer.Send(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &jb.Topic, Partition: kafka.PartitionAny},
		Key:            []byte(strconv.Itoa(jb.Id)),
		Value:          message.Value,
		Timestamp:      time.Now(),
	}); err != nil {
		return err
	}
	if res, err := dq.consumer.CommitMessage(message); err != nil {
		return err
	} else {
		log.Debugf("[InitTW] commit res: %+v", res)
		return nil
	}
}

func (dq *DelayQueue) Run() {
	dq.producer.Run(dq.config.Debug)
	dq.startTimeWheel()

	tickerDuration := time.Second * 2
	readTimeout := time.Millisecond * 100
	ticker := time.NewTicker(tickerDuration) // 每秒检查一次时间轮的大小

	log.Infof("[+] DelayQueue is running, LoTp=%v, HiTp=%v", dq.consumer.LoTp, dq.consumer.HiTp)

	var jb job.Job
	for {
		select {
		case <-ticker.C:

			log.Debug(dq.consumer.Debug(dq.config.Debug))

			if err := dq.consumer.SeekLowOffset(); err != nil {
				log.Error(err)
				break
			}
			log.Debugf("LowTwOffset=%v", dq.consumer.LoTp)
			for dq.tw.Size() < dq.config.MaxTimeWheelSize {
				message, err := dq.consumer.ReadMessage(readTimeout)
				if err != nil {
					if ke, ok := err.(kafka.Error); ok && ke.Code() == kafka.ErrTimedOut {
						//log.Debug("LowOffsetReadTimeout")
					} else {
						log.Error(err)
					}
					break
				}
				if err := json.Unmarshal(message.Value, &jb); err != nil {
					log.Error(err)
				}
				log.Infof("[LowOffsetLoop] recv message=%+v", message)

				offsetKey := utils.Tp2Key(&message.TopicPartition)
				if dq.tw.Find(offsetKey) {
					// 如果在时间轮中，说明未到期
					log.Debugf("[LowOffsetLoop] still in TimeWheel, ignore. message=%+v", message)
					if jb.ExecTimeMs < time.Now().UnixMilli() {
						log.Debugf("[LowOffsetLoop] FATAL: Message timed out, msg=%+v, jb=%s", message, jb.String())
					}
					break
				} else {
					if (dq.consumer.HiTp.Offset < 0) || (message.TopicPartition.Offset < dq.consumer.HiTp.Offset) {
						// 提交丢失，直接消费，投入到目标队列
						log.Debugf("[LowOffsetLoop] handle lost message=%s", message)
						if err := dq.producer.Send(&kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &jb.Topic, Partition: kafka.PartitionAny},
							Key:            []byte(strconv.Itoa(jb.Id)),
							Value:          message.Value,
							Timestamp:      time.Now(),
						}); err != nil {
							log.Error(err)
						}
						if _, err := dq.consumer.CommitMessage(message); err != nil {
							log.Error(err)
						} else {
							//log.Debugf("[LowOffsetLoop] commit message=%+v", res)
						}
						if err := dq.consumer.UpdateOffset(&dq.consumer.LoTp, &message.TopicPartition); err != nil {
							log.Error(err)
						}
					} else {
						log.Debugf("[LowOffsetLoop] msgOffset=%d, highTwOffset=%d", message.TopicPartition.Offset, dq.consumer.HiTp.Offset)
						break
					}
				}
			}

			// 保证LoTP.Offset <= HiTp.Offset
			if dq.consumer.LoTp.Offset > dq.consumer.HiTp.Offset {
				dq.consumer.HiTp.Offset = dq.consumer.LoTp.Offset
			}
			// 从High Offset读取消息到时间轮
			if err := dq.consumer.SeekHighOffset(); err != nil {
				log.Error(err)
				continue
			}
			log.Debugf("HighTwOffset=%+v", dq.consumer.HiTp)
			for dq.tw.Size() < dq.config.MaxTimeWheelSize {
				message, err := dq.consumer.ReadMessage(readTimeout)
				if err != nil {
					if ke, ok := err.(kafka.Error); ok && ke.Code() == kafka.ErrTimedOut {
						//log.Debug("HighOffsetReadTimeout")
					} else {
						log.Error(err)
					}
					break
				}
				if err := json.Unmarshal(message.Value, &jb); err != nil {
					log.Error(err)
				}

				offsetKey := utils.Tp2Key(&message.TopicPartition)
				log.Debugf("[HighOffsetLoop] Put msg=%v into Timewheel", message)
				e := &timewheel.Event{
					Key:      offsetKey,
					Interval: int(jb.DelayMs * int64(time.Millisecond)),
					Cnt:      1,
					RunSync:  true,
					Callback: func() { dq.handleMessage(*message) },
				}
				if err := dq.tw.Put(e); err != nil {
					log.Error(err)
				}
				if err := dq.consumer.UpdateOffset(&dq.consumer.HiTp, &message.TopicPartition); err != nil {
					log.Error(err)
				}
			}
		}
	}
}

func (dq *DelayQueue) Close() {
	dq.producer.Close()
	dq.consumer.Close()
}
