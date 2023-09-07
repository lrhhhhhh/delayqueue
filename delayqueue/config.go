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

type Config struct {
	ProducerConfig producer.Config `yaml:"ProducerConfig"`
	ConsumerConfig consumer.Config `yaml:"ConsumerConfig"`

	NumPartition        int              `yaml:"NumPartition"`
	Replicas            int              `yaml:"Replicas"`
	BatchCommitSize     int              `yaml:"BatchCommitSize"`
	BatchCommitDuration int              `yaml:"BatchCommitDuration"`
	Debug               bool             `yaml:"Debug"`
	Clear               bool             `yaml:"Clear"` // delete topic before run
	DelayTopicFormat    string           `yaml:"DelayTopicFormat"`
	DelayDuration       []string         `yaml:"DelayDuration"`
	TopicPartition      []TopicPartition `yaml:"TopicPartition"`
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

	cfg.fillProducerConfig()

	return cfg, nil
}

func (c *Config) fillProducerConfig() {
	c.ProducerConfig.DelayDuration = make([]string, len(c.DelayDuration))
	copy(c.ProducerConfig.DelayDuration, c.DelayDuration)
	c.ProducerConfig.DelayTopicFormat = c.DelayTopicFormat
}
