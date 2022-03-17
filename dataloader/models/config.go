package models

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type LogConfig struct {
	Level string `yaml:"level"`
}

func (l *LogConfig) SetDefaults() *LogConfig {
	switch l.Level {
	case "DEBUG":
	case "INFO":
	case "ERROR":
	default:
		l.Level = "INFO"
	}
	return l
}

type CredConfig struct {
	Type string      `yaml:"type"`
	Ctx  interface{} `yaml:"context"`
}

func (cc *CredConfig) Validate() error {
	if cc.Type != "" {
		if cc.Ctx == nil {
			return errors.Errorf("Cred is missing")
		}
	}
	return nil
}

type ServerConfig struct {
	Type      string      `yaml:"type"`
	Addresses []string    `yaml:"addresses"`
	Cred      *CredConfig `yaml:"cred"`
	Ctx       interface{} `yaml:"context"`
}

func (sc *ServerConfig) Validate() error {
	if len(sc.Addresses) == 0 {
		return errors.New("Invalid configuration, servers is not setup correctly")
	}

	if sc.Type == "" {
		return errors.New("Invalid configuration, server type is not setup correctly")
	}

	if sc.Cred != nil && sc.Cred.Validate() != nil {
		return errors.New("Invalid configuration, cred is not setup correctly")
	}

	return nil
}

type Settings struct {
	Table             string  `yaml:"table"`
	Topic             string  `yaml:"topic"`
	NumPartitions     int32   `yaml:"num_partitions"`
	ReplicationFactor int16   `yaml:"replication_factor"`
	CleanBeforeLoad   bool    `yaml:"clean_before_load"`
	Concurrency       uint32  `yaml:"concurrency"`
	BatchSize         uint32  `yaml:"batch_size"`
	Interval          uint32  `yaml:"interval"`
	InitialBase       float32 `yaml:"initial_base"`
	Step              float32 `yaml:"step"`
	Duration          uint32  `yaml:"duration"`
	MaxValue          float32 `yaml:"max_value"`
	BackFill          uint32  `yaml:"backfill"`
	TotalEntities     uint32  `yaml:"total_entities"`
	Sourcetype        string  `yaml:"sourcetype"`
	LastRunStateDB    string  `yaml:"last_run_state_db"`
	SampleFile        string  `yaml:"sample_file"`
	Iteration         int32   `yaml:"iteration"`
}

func (s *Settings) SetDefaults() *Settings {
	if s.Concurrency == 0 {
		s.Concurrency = 1
	}

	if s.BatchSize == 0 {
		s.BatchSize = 1000
	}

	if s.TotalEntities == 0 {
		s.TotalEntities = 1
	}

	if s.NumPartitions == 0 {
		s.NumPartitions = 1
	}

	if s.ReplicationFactor == 0 {
		s.ReplicationFactor = 1
	}

	return s
}

type Source struct {
	Name     string   `yaml:"name"`
	Type     string   `yaml:"type"`
	Enabled  bool     `yaml:"enabled"`
	Settings Settings `yaml:"settings"`
}

func (source *Source) Validate() error {
	if source.Name == "" || source.Type == "" {
		return errors.New("name and type are required for data source")
	}

	return nil
}

func (source *Source) SetDefaults() *Source {
	source.Settings.SetDefaults()
	return source
}

type Config struct {
	Sink    ServerConfig `yaml:"sink"`
	Sources []Source     `yaml:"sources"`
	Log     LogConfig    `yaml:"log"`
}

func (c *Config) Validate() error {
	if err := c.Sink.Validate(); err != nil {
		return err
	}

	return nil
}

func (c *Config) SetDefaults() *Config {
	for i := range c.Sources {
		c.Sources[i].SetDefaults()
	}
	c.Log.SetDefaults()
	return c
}

func newConfigFromBytes(data []byte) (*Config, error) {
	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal config data")
	}

	return config, nil
}

func newConfigFromFile(filepath string) (*Config, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read config file")
	}
	return newConfigFromBytes(data)
}

func newConfig(create func() (*Config, error)) (*Config, error) {
	config, err := create()
	if err != nil {
		return nil, err
	}

	if err = config.Validate(); err != nil {
		return nil, err
	}

	config.SetDefaults()
	return config, nil
}

func NewConfigFromFile(filepath string) (*Config, error) {
	create := func() (*Config, error) {
		return newConfigFromFile(filepath)
	}
	return newConfig(create)
}
