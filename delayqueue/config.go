package delayqueue

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"kafkadelayqueue/consumer"
	"kafkadelayqueue/producer"
)

// TopicPartition represent a topic from partition l to r
type TopicPartition struct {
	Topic string `yaml:"topic"`
	L     int    `yaml:"l"`
	R     int    `yaml:"r"`
}

type Common struct {
	ApiVersionRequest bool   `yaml:"api.version.request"`
	BootstrapServers  string `yaml:"bootstrap.servers"`
	SecurityProtocol  string `yaml:"security.protocol"`
	SslCaLocation     string `yaml:"ssl.ca.location"`
	SaslMechanism     string `yaml:"sasl.mechanism"`
	SaslUsername      string `yaml:"sasl.username"`
	SaslPassword      string `yaml:"sasl.password"`
}

type Config struct {
	Common         Common          `yaml:"Common"`
	ProducerConfig producer.Config `yaml:"ProducerConfig"`
	ConsumerConfig consumer.Config `yaml:"ConsumerConfig"`

	DelayQueue struct {
		BatchCommitSize     int              `yaml:"BatchCommitSize"`
		BatchCommitDuration int              `yaml:"BatchCommitDuration"`
		Debug               bool             `yaml:"Debug"`
		DelayTopicFormat    string           `yaml:"DelayTopicFormat"`
		DelayDuration       []string         `yaml:"DelayDuration"`
		TopicPartition      []TopicPartition `yaml:"TopicPartition"`
	} `yaml:"DelayQueue"`
}

func LoadConfig() (*Config, error) {
	yamlData, err := ioutil.ReadFile("./etc/delayqueue.yaml")
	if err != nil {
		return nil, err
	}

	cfg := new(Config)
	err = yaml.Unmarshal(yamlData, cfg)
	if err != nil {
		return nil, err
	}

	cfg.fillConsumerConfig()
	cfg.fillProducerConfig()

	return cfg, nil
}

func (c *Config) fillConsumerConfig() {
	c.ConsumerConfig.ApiVersionRequest = c.Common.ApiVersionRequest
	c.ConsumerConfig.BootstrapServers = c.Common.BootstrapServers
	c.ConsumerConfig.SecurityProtocol = c.Common.SecurityProtocol
	c.ConsumerConfig.SslCaLocation = c.Common.SslCaLocation
	c.ConsumerConfig.SaslMechanism = c.Common.SaslMechanism
	c.ConsumerConfig.SaslUsername = c.Common.SaslUsername
	c.ConsumerConfig.SaslPassword = c.Common.SaslPassword
}

func (c *Config) fillProducerConfig() {
	c.ProducerConfig.ApiVersionRequest = c.Common.ApiVersionRequest
	c.ProducerConfig.BootstrapServers = c.Common.BootstrapServers
	c.ProducerConfig.SecurityProtocol = c.Common.SecurityProtocol
	c.ProducerConfig.SslCaLocation = c.Common.SslCaLocation
	c.ProducerConfig.SaslMechanism = c.Common.SaslMechanism
	c.ProducerConfig.SaslUsername = c.Common.SaslUsername
	c.ProducerConfig.SaslPassword = c.Common.SaslPassword

	c.ProducerConfig.DelayDuration = make([]string, len(c.DelayQueue.DelayDuration))
	copy(c.ProducerConfig.DelayDuration, c.DelayQueue.DelayDuration)
	c.ProducerConfig.DelayTopicFormat = c.DelayQueue.DelayTopicFormat
}
