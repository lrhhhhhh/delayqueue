package producer

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkadelayqueue/job"
	"sort"
	"strconv"
	"time"
)

type Producer struct {
	*kafka.Producer
	c *Config
}

func New(c *Config) (*Producer, error) {
	producer, err := kafka.NewProducer(c.ConfigMap())
	if err != nil {
		return nil, err
	}

	return &Producer{
		Producer: producer,
		c:        c,
	}, nil
}

func (k *Producer) Run(debug bool) {
	go func() {
		for e := range k.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v, message: %+v\n", m.TopicPartition.Error, m)
				} else {
					if debug {
						fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()
}

// Send a message, partition selection using the hash of Key
func (k *Producer) Send(topic string, timestamp time.Time, key, value []byte) (err error) {
	msg := &kafka.Message{
		Timestamp: timestamp,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: value,
	}

	err = k.Producer.Produce(msg, nil)
	if err != nil {
		return err
	}

	// uncomment the code below, block until a resp was received
	//event := <-k.Producer.Events()
	//fmt.Printf("%+v\n", event)
	return nil
}

func (k *Producer) AddJob(jobId, delay int, topic, body string) error {
	j := job.Job{
		Topic:    topic,
		Id:       jobId,
		Delay:    int64(delay),
		ExecTime: int64(delay) + time.Now().Unix(),
		Body:     body,
	}

	err := j.Validate()
	if err != nil {
		return err
	}

	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	delayTopic, err := k.selectDelayTopic(int64(delay))
	if err != nil {
		return err
	}

	return k.Send(
		delayTopic,
		time.Unix(j.ExecTime, 0),
		[]byte(strconv.Itoa(jobId)),
		data,
	)
}

// selectTopic() 为 producer 选择一个大于等于delay的topic, 单位是秒
func (k *Producer) selectDelayTopic(delay int64) (string, error) {
	i := sort.Search(len(k.c.DelayDuration), func(i int) bool {
		d, _ := time.ParseDuration(k.c.DelayDuration[i])
		return d >= time.Duration(delay)*time.Second
	})

	if i == len(k.c.DelayDuration) {
		return "", fmt.Errorf("期望的延迟间隔: %v 大于所有预设的延迟间隔 %v", delay, k.c.DelayDuration)
	}

	return fmt.Sprintf(k.c.DelayTopicFormat, k.c.DelayDuration[i]), nil
}
