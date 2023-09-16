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
	"go.uber.org/zap"
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

	dqc.HiTp = kafka.TopicPartition{
		Topic:     &c.TopicPartition.Topic,
		Partition: int32(c.TopicPartition.Partition),
		Offset:    kafka.OffsetBeginning,
	}
	dqc.LoTp = kafka.TopicPartition{
		Topic:     &c.TopicPartition.Topic,
		Partition: int32(c.TopicPartition.Partition),
		Offset:    kafka.OffsetBeginning,
	}

	if err = dqc.Assign([]kafka.TopicPartition{dqc.LoTp}); err != nil {
		log.Error(err)
		return nil, err
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
	log.Info("handle message", zap.Any("job", jb))

	// todo: lock
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

func (dq *DelayQueue) DebugWatermarkOffsets() {
	if dq.config.Debug {
		topic := dq.config.TopicPartition.Topic
		partition := int32(dq.config.TopicPartition.Partition)
		lo, hi, err := dq.consumer.GetWatermarkOffsets(topic, partition)
		log.Debugf("<topic=%s,partition=%d>-<lo=%d,hi=%d>, err=%v", topic, partition, lo, hi, err)
	}
}

func (dq *DelayQueue) DebugHighPosition() {
	if dq.config.Debug {
		res, err := dq.consumer.Position([]kafka.TopicPartition{dq.consumer.HiTp})
		log.Debugf("position=%v, err=%v", res, err)
	}
}

func (dq *DelayQueue) startTimeWheel() {
	var jb job.Job

	time.Sleep(time.Second * 1)

	dq.DebugWatermarkOffsets()

	if err := dq.consumer.SeekHighOffset(); err != nil {
		log.Error(err)
		panic(err)
	}
	for dq.tw.Size() < dq.config.MaxTimeWheelSize {
		message, err := dq.consumer.ReadMessage(time.Millisecond * 100)
		if err != nil {
			if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrTimedOut {
				log.Debug("Init TimeWheel, ReadTimeout, Skip")
			} else {
				log.Error(err)
			}
			break
		}
		if err = json.Unmarshal(message.Value, &jb); err != nil {
			log.Error(err)
		}
		if jb.ExecTimeMs < time.Now().UnixMilli() {
			log.Debugf("Message timed out, send it immediately, msg=%+v", message)
			if err := dq.producer.Send(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &jb.Topic, Partition: kafka.PartitionAny},
				Value:          message.Value,
				Key:            []byte(strconv.Itoa(jb.Id)),
				Timestamp:      time.Now(),
			}); err != nil {
				log.Error(err)
			}
			if res, err := dq.consumer.CommitMessage(message); err != nil {
				log.Errorf("commit err: %v", err)
			} else {
				log.Debugf("commit res: %+v", res)
			}
		} else {
			// 否则投递到时间轮中
			// 读取m个未超时的消息到时间轮中 （读取但不commit，这里用游标 twOffset标记）
			//（另外，设对应的最后一个为commit的消息的游标为Offset）
			log.Debugf("Put to TimeWheel, msg=%+v", message)
			if err := dq.tw.Put(&timewheel.Event{
				Key:      utils.Tp2Key(&message.TopicPartition), // 记录key=topic-partition-offset是否在时间轮中
				Interval: int(jb.DelayMs * int64(time.Millisecond)),
				Cnt:      1,
				RunSync:  false,
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
	log.Info("[+] TimeWheel is running")
}

func (dq *DelayQueue) Run() {
	dq.producer.Run(dq.config.Debug)
	dq.startTimeWheel()

	tickerDuration := time.Second * 2
	readTimeout := time.Millisecond * 100
	ticker := time.NewTicker(tickerDuration) // 每秒检查一次时间轮的大小

	log.Info("[+] DelayQueue is running")

	var jb job.Job
	for {
		select {
		case <-ticker.C:
			dq.DebugWatermarkOffsets()
			if err := dq.consumer.SeekLowOffset(); err != nil {
				log.Error(err)
				break
			}
			log.Debugf("LowTwOffset=%v", dq.consumer.LoTp)
			for dq.tw.Size() < dq.config.MaxTimeWheelSize {
				message, err := dq.consumer.ReadMessage(readTimeout)
				if err != nil {
					if ke, ok := err.(kafka.Error); ok && ke.Code() == kafka.ErrTimedOut {
						log.Debug("LowOffsetReadTimeout")
					} else {
						log.Error(err)
					}
					break
				}
				if err := json.Unmarshal(message.Value, &jb); err != nil {
					log.Error(err)
				}

				offsetKey := utils.Tp2Key(&message.TopicPartition)
				if dq.tw.Find(offsetKey) { // 如果在时间轮中，说明未到期
					log.Debugf("still wait in TimeWheel, ignore message=%+v", message)
					break
				} else {
					if message.TopicPartition.Offset < dq.consumer.HiTp.Offset { // 提交丢失，直接消费，投入到目标队列
						log.Debugf("handle lost message=%s", message)
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
						}
						if err := dq.consumer.UpdateOffset(&dq.consumer.LoTp, &message.TopicPartition); err != nil {
							log.Error(err)
						}
					} else {
						log.Debugf("msgOffset=%d, highTwOffset=%d", message.TopicPartition.Offset, dq.consumer.HiTp.Offset)
						break
					}
				}
			}

			// 从High Offset读取消息到时间轮
			if err := dq.consumer.SeekHighOffset(); err != nil {
				log.Error(err)
				continue
			}
			log.Debugf("HighTwOffset=%+v", dq.consumer.HiTp)

			dq.DebugHighPosition()

			for dq.tw.Size() < dq.config.MaxTimeWheelSize {
				message, err := dq.consumer.ReadMessage(readTimeout)
				if err != nil {
					if ke, ok := err.(kafka.Error); ok && ke.Code() == kafka.ErrTimedOut {
						log.Debug("HighOffsetReadTimeout")
					} else {
						log.Error(err)
					}
					break
				}
				if err := json.Unmarshal(message.Value, &jb); err != nil {
					log.Error(err)
				}

				offsetKey := utils.Tp2Key(&message.TopicPartition)
				log.Debugf("Put msg=%v into Timewheel", message)
				if err := dq.tw.Put(&timewheel.Event{
					Key:      offsetKey,
					Interval: int(jb.DelayMs * int64(time.Millisecond)),
					Cnt:      1,
					RunSync:  false,
					Callback: func() { dq.handleMessage(*message) },
				}); err != nil {
					log.Error(err, zap.Any("job", jb))
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
