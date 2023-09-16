package utils

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Tp2Key kafka.TopicPartition to topic-partition-offset string key
func Tp2Key(tp *kafka.TopicPartition) string {
	return fmt.Sprintf("%s-%d-%d", *tp.Topic, tp.Partition, tp.Offset)
}
