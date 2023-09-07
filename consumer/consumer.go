package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer 用于消费延迟主题中的所有消息
type Consumer struct {
	*kafka.Consumer
	config *Config
}

func New(c *Config) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(c.ConfigMap())
	if err != nil {
		return nil, err
	}
	return &Consumer{Consumer: consumer, config: c}, nil
}
