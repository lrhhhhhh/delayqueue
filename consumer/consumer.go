package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	*kafka.Consumer
	c *Config
}

func New(c *Config) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(c.ConfigMap())
	if err != nil {
		return nil, err
	}
	return &Consumer{Consumer: consumer, c: c}, nil
}
