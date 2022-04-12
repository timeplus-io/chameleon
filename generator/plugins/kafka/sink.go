package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const KAFKA_SINK_TYPE = "kafka"

type KafkaSink struct {
	brokers []string
	topic   string

	client *kgo.Client
	ctx    context.Context
}

func NewKafkaSink(properties map[string]interface{}) (sink.Sink, error) {
	brokers, err := utils.GetWithDefault(properties, "brokers", "localhost:9092")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &KafkaSink{
		brokers: strings.Split(brokers, ","),
		ctx:     context.Background(),
	}, nil
}

func (s *KafkaSink) Init(name string, fields []common.Field) error {
	s.topic = name
	client, err := kgo.NewClient(
		kgo.SeedBrokers(s.brokers...),
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return err
	}

	s.client = client

	admClient := kadm.NewClient(s.client)
	admClient.DeleteTopics(s.ctx, s.topic)

	//s.client.PurgeTopicsFromClient(s.topic)
	return nil
}

func (s *KafkaSink) Write(headers []string, rows [][]interface{}, index int) error {
	events := common.ToEvents(headers, rows)
	for _, event := range events {
		log.Logger().Debugf("writing event to kafka %s", fmt.Sprintf("%v", event))
		eventValue, _ := json.Marshal(event)
		record := &kgo.Record{Topic: s.topic, Value: eventValue}
		s.client.Produce(s.ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Logger().Errorf("record had a produce error: %w", err)
			}
		})
	}
	return nil
}
