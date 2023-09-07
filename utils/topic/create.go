package topic

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

// Create 使用 admin 创建 kafka topic 和 partition
func Create(admin *kafka.AdminClient, topics []string, numPartition, replica int) {
	for _, topic := range topics {
		results, err := admin.CreateTopics(
			context.Background(),
			[]kafka.TopicSpecification{{
				Topic:             topic,
				NumPartitions:     numPartition, // NOTE:
				ReplicationFactor: replica}},
			kafka.SetAdminOperationTimeout(time.Millisecond*200))
		if err != nil {
			log.Printf("Failed to create topic: %v\n", err)
		}

		for _, result := range results {
			log.Printf("%s\n", result)
		}
	}
	log.Println("create topics done")
}
