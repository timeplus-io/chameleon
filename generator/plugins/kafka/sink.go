package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const KAFKA_SINK_TYPE = "kafka"

type KafkaSink struct {
	brokers      []string
	topic        string
	tls          bool
	sasl         string
	saslUsername string
	saslPassword string

	client *kgo.Client
	ctx    context.Context
}

func NewKafkaSink(properties map[string]interface{}) (sink.Sink, error) {
	brokers, err := utils.GetWithDefault(properties, "brokers", "localhost:9092")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	tls, err := utils.GetBoolWithDefault(properties, "tls", false)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	sasl, err := utils.GetWithDefault(properties, "sasl", KAFKA_SASL_TYPE_NONE)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	saslUsername, err := utils.GetWithDefault(properties, "username", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	saslPassword, err := utils.GetWithDefault(properties, "password", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &KafkaSink{
		brokers:      strings.Split(brokers, ","),
		tls:          tls,
		sasl:         sasl,
		saslUsername: saslUsername,
		saslPassword: saslPassword,
		ctx:          context.Background(),
	}, nil
}

func (s *KafkaSink) Init(name string, fields []common.Field) error {
	s.topic = name
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.AllowAutoTopicCreation(),
	}

	if s.tls {
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	if s.sasl == KAFKA_SASL_TYPE_PLAIN {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: s.saslUsername,
			Pass: s.saslPassword,
		}.AsMechanism()))
	} else if s.sasl == KAFKA_SASL_TYPE_SCRAM {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: s.saslUsername,
			Pass: s.saslPassword,
		}.AsSha512Mechanism()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	s.client = client

	return nil
}

func (s *KafkaSink) cleanTopic() {
	admClient := kadm.NewClient(s.client)
	admClient.DeleteTopics(s.ctx, s.topic)
	s.client.PurgeTopicsFromClient(s.topic)
}

func (s *KafkaSink) Write(headers []string, rows [][]interface{}, index int) error {
	events := common.ToEvents(headers, rows)
	for _, event := range events {
		log.Logger().Debugf("writing event to kafka topic %s, event %s", s.topic, fmt.Sprintf("%v", event))
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
