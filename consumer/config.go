package consumer

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Config struct {
	AutoOffsetReset        string `yaml:"auto.offset.reset"`
	EnableAutoCommit       string `yaml:"enable.auto.commit"`
	FetchMaxBytes          string `yaml:"fetch.max.bytes"`
	GroupId                string `yaml:"group.id"`
	HeartbeatIntervalMs    string `yaml:"heartbeat.interval.ms"`
	MaxPollIntervalMs      string `yaml:"max.poll.interval.ms"`
	MaxPartitionFetchBytes string `yaml:"max.partition.fetch.bytes"`
	SessionTimeoutMs       string `yaml:"session.timeout.ms"`

	ApiVersionRequest bool   `yaml:"api.version.request,omitempty"`
	BootstrapServers  string `yaml:"bootstrap.servers,omitempty"`
	SecurityProtocol  string `yaml:"security.protocol,omitempty"`
	SslCaLocation     string `yaml:"ssl.ca.location,omitempty"`
	SaslMechanism     string `yaml:"sasl.mechanism,omitempty"`
	SaslUsername      string `yaml:"sasl.username,omitempty"`
	SaslPassword      string `yaml:"sasl.password,omitempty"`
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
