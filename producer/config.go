package producer

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Config struct {
	Acks                       string `yaml:"acks"`
	BatchSize                  int    `yaml:"batch.size"`
	CompressionType            string `yaml:"compression.type"`
	DeliveryTimeoutMs          string `yaml:"delivery.timeout.ms"`
	LingerMs                   string `yaml:"linger.ms"`
	MessageMaxBytes            string `yaml:"message.max.bytes"`
	Retries                    string `yaml:"retries"`
	RetryBackoffMs             string `yaml:"retry.backoff.ms"`
	StickyPartitioningLingerMs string `yaml:"sticky.partitioning.linger.ms"`

	ApiVersionRequest bool   `yaml:"api.version.request,omitempty"`
	BootstrapServers  string `yaml:"bootstrap.servers,omitempty"`
	SecurityProtocol  string `yaml:"security.protocol,omitempty"`
	SslCaLocation     string `yaml:"ssl.ca.location,omitempty"`
	SaslMechanism     string `yaml:"sasl.mechanism,omitempty"`
	SaslUsername      string `yaml:"sasl.username,omitempty"`
	SaslPassword      string `yaml:"sasl.password,omitempty"`

	DelayTopicFormat string   `yaml:"DelayTopicFormat"`
	DelayDuration    []string `yaml:"DelayDuration"`
}

func (c *Config) ConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"acks":                          c.Acks,
		"batch.size":                    c.BatchSize,
		"compression.type":              c.CompressionType,
		"delivery.timeout.ms":           c.DeliveryTimeoutMs,
		"linger.ms":                     c.LingerMs,
		"message.max.bytes":             c.MessageMaxBytes,
		"retries":                       c.Retries,
		"retry.backoff.ms":              c.RetryBackoffMs,
		"sticky.partitioning.linger.ms": c.StickyPartitioningLingerMs,

		"api.version.request": c.ApiVersionRequest,
		"bootstrap.servers":   c.BootstrapServers,
		"security.protocol":   c.SecurityProtocol,

		// NOTE: uncomment the code below if `security.protocol != PLAINTEXT`
		//"ssl.ca.location": c.KafkaCommon.SslCaLocation,
		//"sasl.mechanisms": c.KafkaCommon.SaslMechanism,
		//"sasl.username": c.KafkaCommon.SaslUsername,
		//"sasl.password": c.KafkaCommon.SaslPassword,
	}
}
