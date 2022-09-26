package delayqueue

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaConsumer struct {
	*kafka.Consumer
}

func NewKafkaConsumer(kafkaConf *kafka.ConfigMap) (*kafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(kafkaConf)
	if err != nil {
		return nil, err
	}
	return &kafkaConsumer{Consumer: consumer}, nil
}
