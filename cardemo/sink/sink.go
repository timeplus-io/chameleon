package sink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/timeplus-io/chameleon/cardemo/log"
	"github.com/timeplus-io/chameleon/cardemo/sink/kafka"
	"github.com/timeplus-io/chameleon/cardemo/sink/proton"
	"github.com/timeplus-io/chameleon/cardemo/sink/timeplus"
	"sigs.k8s.io/yaml"
)

func LoadSinks(path string) ([]Sink, error) {
	config, err := loadConfig(path)
	if err != nil {
		return nil, err
	}

	result := make([]Sink, len(config.SinkConfigs))
	for index, config := range config.SinkConfigs {
		if config.Type == "kafka" {
			if result[index], err = kafka.NewKafkaSink(config.Properties); err != nil {
				return nil, err
			}
		} else if config.Type == "timeplus" {
			if result[index], err = timeplus.NewTimeplusSink(config.Properties); err != nil {
				return nil, err
			}
		} else if config.Type == "proton" {
			if result[index], err = proton.NewProtonSink(config.Properties); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func loadConfig(path string) (*JobConfig, error) {
	dat, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var payload JobConfig
	if strings.HasSuffix(path, ".json") {
		json.NewDecoder(bytes.NewBuffer(dat)).Decode(&payload)
		return &payload, nil
	} else if strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml") {
		err := yaml.Unmarshal(dat, &payload)
		if err != nil {
			return nil, err
		}
		log.Logger().Infof("loaded config is %v", payload)
		return &payload, nil
	}

	return nil, fmt.Errorf("configuration has to be json or yaml")
}
