package consumer

import (
	"delayqueue/log"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
	"time"
)

var ErrInvalidTP = errors.New("not the same topic-partition")
var ErrInvalidTPOffset = errors.New("invalid offset")

type Consumer struct {
	*kafka.Consumer
	LoTp   kafka.TopicPartition // LowTopicPartition
	HiTp   kafka.TopicPartition // HighTopicPartition
	locker *sync.Mutex
	Config Config
}

func New(c Config) (*Consumer, error) {
	kafkaConsumer, err := kafka.NewConsumer(c.ConfigMap())
	if err != nil {
		return nil, err
	}

	offsets, err := kafkaConsumer.Committed([]kafka.TopicPartition{{
		Topic:     &c.TopicPartition.Topic,
		Partition: int32(c.TopicPartition.Partition),
		//Offset:    kafka.OffsetBeginning,
	}}, 1000)
	if err != nil {
		return nil, err
	}

	if offsets[0].Offset == kafka.OffsetInvalid {
		offsets[0].Offset = kafka.OffsetBeginning
	}

	log.Infof("Consumer start from topic-partition-offset=%v", offsets[0])

	if err = kafkaConsumer.Assign(offsets); err != nil {
		return nil, err
	}
	return &Consumer{
		Consumer: kafkaConsumer,
		LoTp:     offsets[0],
		HiTp:     offsets[0],
		Config:   c,
		locker:   new(sync.Mutex),
	}, nil
}

func (c *Consumer) Close() {
	if err := c.Consumer.Close(); err != nil {
		panic(err)
	}
}

func (c *Consumer) UpdateOffset(old, new *kafka.TopicPartition) error {
	if *old.Topic != *new.Topic || old.Partition != new.Partition {
		return ErrInvalidTP
	}
	if old.Offset > new.Offset {
		log.Debugf("oldOffset=%d, newOffset=%d", old.Offset, new.Offset)
		return ErrInvalidTPOffset
	}
	c.locker.Lock()
	defer c.locker.Unlock()
	old.Offset = new.Offset + 1
	return nil
}

func (c *Consumer) SeekLowOffset() error {
	c.locker.Lock()
	defer c.locker.Unlock()
	return c.Seek(c.LoTp, 200)
}

func (c *Consumer) SeekHighOffset() error {
	c.locker.Lock()
	defer c.locker.Unlock()
	return c.Seek(c.HiTp, 200)
}

func (c *Consumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	c.locker.Lock()
	defer c.locker.Unlock()
	return c.Consumer.ReadMessage(timeout)
}

func (c *Consumer) CommitMessage(message *kafka.Message) ([]kafka.TopicPartition, error) {
	c.locker.Lock()
	defer c.locker.Unlock()
	return c.Consumer.CommitMessage(message)
}

func (c *Consumer) Debug(flag bool) string {
	if !flag {
		return ""
	}
	c.locker.Lock()
	defer c.locker.Unlock()
	topic := c.Config.TopicPartition.Topic
	partition := int32(c.Config.TopicPartition.Partition)
	lo, hi, err := c.GetWatermarkOffsets(topic, partition)
	if err != nil {
		panic(err)
	}

	offsets, err := c.Committed([]kafka.TopicPartition{c.HiTp}, 500)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("last committed offset=%v;  lo=%d, hi=%d", offsets, lo, hi)
}
