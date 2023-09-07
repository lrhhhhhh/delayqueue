package topic

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

// Delete 使用 admin 删除 kafka topic 和 partition
func Delete(admin *kafka.AdminClient, topics []kafka.TopicSpecification) {
	topicNames := make([]string, len(topics))
	for i, x := range topics {
		topicNames[i] = x.Topic
	}
	results, err := admin.DeleteTopics(context.Background(), topicNames)
	if err != nil {
		panic(err)
	}
	for _, result := range results {
		log.Printf("%s\n", result)
	}
	log.Println("delete topics done")
}
