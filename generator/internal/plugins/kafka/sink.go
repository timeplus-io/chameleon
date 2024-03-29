package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/timeplus-io/chameleon/generator/internal/common"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/utils"
)

const KAFKA_SINK_TYPE = "kafka"

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type KafkaSink struct {
	brokers      []string
	topic        string
	tls          bool
	sasl         string
	saslUsername string
	saslPassword string
	createTopic  bool

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

	createTopic, err := utils.GetBoolWithDefault(properties, "create_topic", false)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &KafkaSink{
		brokers:      strings.Split(brokers, ","),
		tls:          tls,
		sasl:         sasl,
		saslUsername: saslUsername,
		saslPassword: saslPassword,
		createTopic:  createTopic,
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
	if s.createTopic {
		admClient := kadm.NewClient(s.client)
		if _, err := admClient.CreateTopics(s.ctx, 1, 3, nil, s.topic); err != nil {
			return err
		}
	}

	return nil
}

func (s *KafkaSink) cleanTopic() {
	admClient := kadm.NewClient(s.client)
	admClient.DeleteTopics(s.ctx, s.topic)
	s.client.PurgeTopicsFromClient(s.topic)
}

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (s *KafkaSink) Write(headers []string, rows [][]interface{}, index int) error {
	events := common.ToEvents(headers, rows)
	for _, event := range events {
		log.Logger().Debugf("writing event to kafka topic %s, event %s", s.topic, fmt.Sprintf("%v", event))
		eventValue, _ := json.Marshal(event)
		key := []byte(randStringBytes(8))
		record := &kgo.Record{Topic: s.topic, Value: eventValue, Key: key}
		s.client.Produce(s.ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Logger().Errorf("record had a produce error: %s", err)
			}
		})
	}
	return nil
}

func (s *KafkaSink) GetStats() *sink.Stats {
	return &sink.Stats{
		SuccessWrite: 0,
		FailedWrite:  0,
	}
}
