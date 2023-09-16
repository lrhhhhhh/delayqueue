package consumer

import (
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LoHiConsumer struct {
	*kafka.Consumer
	LoTp   kafka.TopicPartition // LowTopicPartition
	HiTp   kafka.TopicPartition // HighTopicPartition
	Config Config
}

func NewLoHiConsumer(c Config) (*LoHiConsumer, error) {
	consumer, err := kafka.NewConsumer(c.ConfigMap())
	if err != nil {
		return nil, err
	}
	return &LoHiConsumer{
		Consumer: consumer,
		LoTp:     kafka.TopicPartition{},
		HiTp:     kafka.TopicPartition{},
		Config:   c,
	}, nil
}

func (c *LoHiConsumer) Close() {
	if err := c.Consumer.Close(); err != nil {
		panic(err)
	}
}

var ErrInvalidTP = errors.New("not the same topic-partition")
var ErrInvalidTPOffset = errors.New("invalid offset")

func (c *LoHiConsumer) UpdateOffset(old, new *kafka.TopicPartition) error {
	if *old.Topic != *new.Topic || old.Partition != new.Partition {
		return ErrInvalidTP
	}
	if old.Offset > new.Offset {
		return ErrInvalidTPOffset
	}
	old.Offset = new.Offset + 1
	return nil
}

func (c *LoHiConsumer) SeekLowOffset() error {
	return c.Seek(c.LoTp, 200)
}

func (c *LoHiConsumer) SeekHighOffset() error {
	return c.Seek(c.HiTp, 200)
}
