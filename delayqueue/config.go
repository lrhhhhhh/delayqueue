package delayqueue

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

// TopicPartition represent a topic from partition l to r
type TopicPartition struct {
	Topic string `yaml:"topic"`
	L     int    `yaml:"l"`
	R     int    `yaml:"r"`
}

type KafkaDelayQueueConfig struct {
	KafkaCommon struct {
		ApiVersionRequest bool   `yaml:"api.version.request"`
		BootstrapServers  string `yaml:"bootstrap.servers"`
		SecurityProtocol  string `yaml:"security.protocol"`
		SslCaLocation     string `yaml:"ssl.ca.location"`
		SaslMechanism     string `yaml:"sasl.mechanism"`
		SaslUsername      string `yaml:"sasl.username"`
		SaslPassword      string `yaml:"sasl.password"`
	} `yaml:"KafkaCommon"`

	KafkaProducer struct {
		Acks                       string `yaml:"acks"`
		BatchSize                  int    `yaml:"batch.size"`
		CompressionType            string `yaml:"compression.type"`
		DeliveryTimeoutMs          string `yaml:"delivery.timeout.ms"`
		LingerMs                   string `yaml:"linger.ms"`
		MessageMaxBytes            string `yaml:"message.max.bytes"`
		Retries                    string `yaml:"retries"`
		RetryBackoffMs             string `yaml:"retry.backoff.ms"`
		StickyPartitioningLingerMs string `yaml:"sticky.partitioning.linger.ms"`
	} `yaml:"KafkaProducer"`

	KafkaConsumer struct {
		AutoOffsetReset        string `yaml:"auto.offset.reset"`
		EnableAutoCommit       string `yaml:"enable.auto.commit"`
		FetchMaxBytes          string `yaml:"fetch.max.bytes"`
		GroupId                string `yaml:"group.id"`
		HeartbeatIntervalMs    string `yaml:"heartbeat.interval.ms"`
		MaxPollIntervalMs      string `yaml:"max.poll.interval.ms"`
		MaxPartitionFetchBytes string `yaml:"max.partition.fetch.bytes"`
		SessionTimeoutMs       string `yaml:"session.timeout.ms"`
	} `yaml:"KafkaConsumer"`

	DelayQueue struct {
		BatchCommitSize     int              `yaml:"BatchCommitSize"`
		BatchCommitDuration int              `yaml:"BatchCommitDuration"`
		Debug               bool             `yaml:"Debug"`
		DelayTopicFormat    string           `yaml:"DelayTopicFormat"`
		DelayDuration       []string         `yaml:"DelayDuration"`
		TopicPartition      []TopicPartition `yaml:"TopicPartition"`
	} `yaml:"DelayQueue"`
}

func NewKafkaProducerConfig(c *KafkaDelayQueueConfig) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"acks":                          c.KafkaProducer.Acks,
		"batch.size":                    c.KafkaProducer.BatchSize,
		"compression.type":              c.KafkaProducer.CompressionType,
		"delivery.timeout.ms":           c.KafkaProducer.DeliveryTimeoutMs,
		"linger.ms":                     c.KafkaProducer.LingerMs,
		"message.max.bytes":             c.KafkaProducer.MessageMaxBytes,
		"retries":                       c.KafkaProducer.Retries,
		"retry.backoff.ms":              c.KafkaProducer.RetryBackoffMs,
		"sticky.partitioning.linger.ms": c.KafkaProducer.StickyPartitioningLingerMs,

		"api.version.request": c.KafkaCommon.ApiVersionRequest,
		"bootstrap.servers":   c.KafkaCommon.BootstrapServers,
		"security.protocol":   c.KafkaCommon.SecurityProtocol,

		// NOTE: uncomment the code below if `security.protocol != PLAINTEXT`
		//"ssl.ca.location": c.KafkaCommon.SslCaLocation,
		//"sasl.mechanisms": c.KafkaCommon.SaslMechanism,
		//"sasl.username": c.KafkaCommon.SaslUsername,
		//"sasl.password": c.KafkaCommon.SaslPassword,
	}
}

func NewKafkaConsumerConfig(c *KafkaDelayQueueConfig) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"auto.offset.reset":         c.KafkaConsumer.AutoOffsetReset,
		"enable.auto.commit":        c.KafkaConsumer.EnableAutoCommit,
		"fetch.max.bytes":           c.KafkaConsumer.FetchMaxBytes,
		"group.id":                  c.KafkaConsumer.GroupId,
		"heartbeat.interval.ms":     c.KafkaConsumer.HeartbeatIntervalMs,
		"max.partition.fetch.bytes": c.KafkaConsumer.MaxPartitionFetchBytes,
		"max.poll.interval.ms":      c.KafkaConsumer.MaxPollIntervalMs,
		"session.timeout.ms":        c.KafkaConsumer.SessionTimeoutMs,

		"api.version.request": c.KafkaCommon.ApiVersionRequest,
		"bootstrap.servers":   c.KafkaCommon.BootstrapServers,
		"security.protocol":   c.KafkaCommon.SecurityProtocol,

		// NOTE: uncomment the code below if `security.protocol != PLAINTEXT`
		//"ssl.ca.location":     c.KafkaCommon.SslCaLocation,
		//"sasl.mechanisms":     c.KafkaCommon.SaslMechanism,
		//"sasl.username":       c.KafkaCommon.SaslUsername,
		//"sasl.password":       c.KafkaCommon.SaslPassword,
	}
}

func NewKafkaDelayQueueConfig() *KafkaDelayQueueConfig {
	content, err := ioutil.ReadFile("./etc/delayqueue.yaml")
	if err != nil {
		panic(err)
	}

	c := &KafkaDelayQueueConfig{}
	err = yaml.Unmarshal(content, c)
	if err != nil {
		panic(err)
	}

	return c
}
