package producer

import (
	"delayqueue/job"
	"delayqueue/log"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sort"
	"strconv"
	"time"
)

type Producer struct {
	*kafka.Producer
	config *Config
}

func New(c *Config) (*Producer, error) {
	producer, err := kafka.NewProducer(c.ConfigMap())
	if err != nil {
		return nil, err
	}

	return &Producer{
		Producer: producer,
		config:   c,
	}, nil
}

func (p *Producer) Run(debug bool) {
	// 监听结果
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					log.Errorf("Delivery failed: %v, message: %+v\n", m.TopicPartition.Error, m)
				} else {
					log.Debugf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				log.Errorf("Error: %v\n", ev)
			default:
				log.Errorf("Ignored event: %s\n", ev)
			}
		}
	}()
}

// Send a message, partition selection using the hash of Key
func (p *Producer) Send(message *kafka.Message) (err error) {
	//topic string, timestamp time.Time, key, value []byte
	//msg := &kafka.Message{
	//	Timestamp: timestamp,
	//	TopicPartition: kafka.TopicPartition{
	//		Topic:     &topic,
	//		Partition: kafka.PartitionAny,
	//	},
	//	Key:   key,
	//	Value: value,
	//}

	err = p.Producer.Produce(message, nil)
	if err != nil {
		return err
	}

	// uncomment the code below, block until a resp was received
	// 如果运行了Run()，那么这里会被阻塞
	//event := <-p.Events()
	//fmt.Printf("%+v\n", event)
	return nil
}

//func (p *Producer) Send(topic string, timestamp time.Time, key, value []byte) (err error) {
//	msg := &kafka.Message{
//		Timestamp: timestamp,
//		TopicPartition: kafka.TopicPartition{
//			Topic:     &topic,
//			Partition: kafka.PartitionAny,
//		},
//		Key:   key,
//		Value: value,
//	}
//
//	err = p.Producer.Produce(msg, nil)
//	if err != nil {
//		return err
//	}
//
//	// uncomment the code below, block until a resp was received
//	// 如果运行了Run()，那么这里会被阻塞
//	//event := <-p.Events()
//	//fmt.Printf("%+v\n", event)
//	return nil
//}

func (p *Producer) AddJob(jb *job.Job) error {
	//jobId, delayMs int, topic, body string
	//j := job.Job{
	//	Topic:      topic,
	//	Id:         jobId,
	//	DelayMs:    int64(delayMs),
	//	ExecTimeMs: int64(delayMs) + time.Now().UnixMilli(),
	//	Body:       body,
	//}

	if err := jb.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(jb)
	if err != nil {
		return err
	}

	delayTopic, err := p.selectDelayTopic(int(jb.DelayMs))
	if err != nil {
		return err
	}

	return p.Send(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &delayTopic,
			Partition: kafka.PartitionAny,
		},
		Value:     data,
		Key:       []byte(strconv.Itoa(jb.Id)),
		Timestamp: time.UnixMilli(jb.ExecTimeMs),
	})
}

//func (p *Producer) AddJob(jobId, delayMs int, topic, body string) error {
//	j := job.Job{
//		Topic:      topic,
//		Id:         jobId,
//		DelayMs:    int64(delayMs),
//		ExecTimeMs: int64(delayMs) + time.Now().UnixMilli(),
//		Body:       body,
//	}
//
//	err := j.Validate()
//	if err != nil {
//		return err
//	}
//
//	data, err := json.Marshal(j)
//	if err != nil {
//		return err
//	}
//
//	delayTopic, err := p.selectDelayTopic(delayMs)
//	if err != nil {
//		return err
//	}
//
//	//log.Debug(delayTopic)
//
//	return p.Send(
//		delayTopic,
//		time.UnixMilli(j.ExecTimeMs),
//		[]byte(strconv.Itoa(jobId)),
//		data,
//	)
//}

// selectTopic() 为 producer 选择一个大于等于delay的topic, 单位是毫秒
func (p *Producer) selectDelayTopic(delayMs int) (string, error) {
	i := sort.Search(len(p.config.DelayDuration), func(i int) bool {
		d, _ := time.ParseDuration(p.config.DelayDuration[i])
		return d >= time.Duration(delayMs)*time.Millisecond
	})

	if i == len(p.config.DelayDuration) {
		return "", fmt.Errorf("期望的延迟间隔: %v 大于所有预设的延迟间隔 %v", delayMs, p.config.DelayDuration)
	}

	return fmt.Sprintf(p.config.DelayTopicFormat, p.config.DelayDuration[i]), nil
}
