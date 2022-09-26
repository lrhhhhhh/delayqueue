package delayqueue

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sort"
	"strconv"
	"time"
)

type kafkaProducer struct {
	*kafka.Producer
	DelayDuration    []string
	DelayTopicFormat string
}

func NewKafkaProducer(c *KafkaDelayQueueConfig) (*kafkaProducer, error) {
	cfg := NewKafkaProducerConfig(c)
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	DelayDurationCopy := make([]string, len(c.DelayQueue.DelayDuration))
	copy(DelayDurationCopy, c.DelayQueue.DelayDuration)

	return &kafkaProducer{
		Producer:         producer,
		DelayDuration:    DelayDurationCopy,
		DelayTopicFormat: c.DelayQueue.DelayTopicFormat,
	}, nil
}

func (k *kafkaProducer) Run(debug bool) {
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
func (k *kafkaProducer) Send(topic string, timestamp time.Time, key, value []byte) (err error) {
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

func (k *kafkaProducer) AddJob(jobId, delay int, topic, body string) error {
	job := Job{
		Topic:    topic,
		Id:       jobId,
		Delay:    int64(delay),
		ExecTime: int64(delay) + time.Now().Unix(),
		Body:     body,
	}

	err := job.Validate()
	if err != nil {
		return err
	}

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	delayTopic, err := k.selectDelayTopic(int64(delay))
	if err != nil {
		return err
	}

	return k.Send(
		delayTopic,
		time.Unix(job.ExecTime, 0),
		[]byte(strconv.Itoa(jobId)),
		data,
	)
}

// selectTopic() 为 producer 选择一个大于等于delay的topic, 单位是秒
func (k *kafkaProducer) selectDelayTopic(delay int64) (string, error) {
	i := sort.Search(len(k.DelayDuration), func(i int) bool {
		d, _ := time.ParseDuration(k.DelayDuration[i])
		return d >= time.Duration(delay)*time.Second
	})

	if i == len(k.DelayDuration) {
		return "", fmt.Errorf("期望的延迟间隔: %v 大于所有预设的延迟间隔 %v", delay, k.DelayDuration)
	}

	return fmt.Sprintf(k.DelayTopicFormat, k.DelayDuration[i]), nil
}
