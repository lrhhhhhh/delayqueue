package topic

import (
	"context"
	"delayqueue/log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

// Create 使用 admin 创建 kafka topic 和 partition
func Create(admin *kafka.AdminClient, topics []kafka.TopicSpecification) {
	results, err := admin.CreateTopics(
		context.Background(),
		topics,
		kafka.SetAdminOperationTimeout(time.Millisecond*200))
	if err != nil {
		log.Errorf("Failed to create topic: %v\n", err)
	}

	for _, result := range results {
		log.Warnf("%s\n", result)
	}
	log.Info("create topics done")
}
