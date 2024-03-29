package consumer

import "github.com/confluentinc/confluent-kafka-go/kafka"

type TopicPartition struct {
	Topic     string `yaml:"topic"`
	Partition int64  `yaml:"partition"`
}

type Config struct {
	AutoOffsetReset        string `yaml:"auto.offset.reset"`
	EnableAutoCommit       string `yaml:"enable.auto.commit"`
	FetchMaxBytes          string `yaml:"fetch.max.bytes"`
	GroupId                string `yaml:"group.id"`
	HeartbeatIntervalMs    string `yaml:"heartbeat.interval.ms"`
	MaxPollIntervalMs      string `yaml:"max.poll.interval.ms"`
	MaxPartitionFetchBytes string `yaml:"max.partition.fetch.bytes"`
	SessionTimeoutMs       string `yaml:"session.timeout.ms"`

	ApiVersionRequest bool   `yaml:"api.version.request"`
	BootstrapServers  string `yaml:"bootstrap.servers"`
	SecurityProtocol  string `yaml:"security.protocol"`
	SslCaLocation     string `yaml:"ssl.ca.location"`
	SaslMechanism     string `yaml:"sasl.mechanism"`
	SaslUsername      string `yaml:"sasl.username"`
	SaslPassword      string `yaml:"sasl.password"`

	TopicPartition TopicPartition `yaml:"TopicPartition"`
}

func (c *Config) ConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"auto.offset.reset":         c.AutoOffsetReset,
		"enable.auto.commit":        c.EnableAutoCommit,
		"fetch.max.bytes":           c.FetchMaxBytes,
		"group.id":                  c.GroupId,
		"heartbeat.interval.ms":     c.HeartbeatIntervalMs,
		"max.partition.fetch.bytes": c.MaxPartitionFetchBytes,
		"max.poll.interval.ms":      c.MaxPollIntervalMs,
		"session.timeout.ms":        c.SessionTimeoutMs,

		"api.version.request": c.ApiVersionRequest,
		"bootstrap.servers":   c.BootstrapServers,
		"security.protocol":   c.SecurityProtocol,

		// NOTE: uncomment the code below if `security.protocol != PLAINTEXT`
		//"ssl.ca.location":     c.KafkaCommon.SslCaLocation,
		//"sasl.mechanisms":     c.KafkaCommon.SaslMechanism,
		//"sasl.username":       c.KafkaCommon.SaslUsername,
		//"sasl.password":       c.KafkaCommon.SaslPassword,
	}
}
