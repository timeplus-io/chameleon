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

	"github.com/timeplus-io/chameleon/cardemo/common"
	"github.com/timeplus-io/chameleon/cardemo/log"
	"github.com/timeplus-io/chameleon/cardemo/utils"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const KAFKA_SASL_TYPE_NONE = "none"
const KAFKA_SASL_TYPE_PLAIN = "plain"
const KAFKA_SASL_TYPE_SCRAM = "scram"
const KAFKA_SASL_TYPE_SCRAM_256 = "scram256"
const KAFKA_SASL_TYPE_SCRAM_512 = "scram512"
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type KafkaSink struct {
	brokers      []string
	tls          bool
	sasl         string
	saslUsername string
	saslPassword string

	client *kgo.Client
	ctx    context.Context
}

func NewKafkaSink(properties map[string]any) (*KafkaSink, error) {
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

	log.Logger().Infof("kafka sink inititalize with broker %s", brokers)

	return &KafkaSink{
		brokers:      strings.Split(brokers, ","),
		tls:          tls,
		sasl:         sasl,
		saslUsername: saslUsername,
		saslPassword: saslPassword,
		ctx:          context.Background(),
	}, nil
}

func (s *KafkaSink) Init() error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.AllowAutoTopicCreation(),
	}

	if s.tls {
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))

		tlsConfig := kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true})
		opts = append(opts, tlsConfig)
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
	} else if s.sasl == KAFKA_SASL_TYPE_SCRAM_512 {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: s.saslUsername,
			Pass: s.saslPassword,
		}.AsSha512Mechanism()))
	} else if s.sasl == KAFKA_SASL_TYPE_SCRAM_256 {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: s.saslUsername,
			Pass: s.saslPassword,
		}.AsSha256Mechanism()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}

	s.client = client
	return nil
}

func (s *KafkaSink) InitCars(cars []*common.DimCar) error {
	go s.initCars(cars)
	return nil
}

func (s *KafkaSink) initCars(cars []*common.DimCar) error {
	for _, car := range cars {
		event := car.ToEvent()
		eventValue, _ := json.Marshal(event)
		key := []byte(randStringBytes(8))
		record := &kgo.Record{Topic: "dimcar", Value: eventValue, Key: key}
		s.client.Produce(s.ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Logger().Errorf("record had a produce error: %s", err)
			}
		})
	}
	return nil
}

func (s *KafkaSink) InitUsers(users []*common.DimUser) error {
	go s.initUsers(users)
	return nil
}

func (s *KafkaSink) initUsers(users []*common.DimUser) error {
	for _, user := range users {
		event := user.ToEvent()
		eventValue, _ := json.Marshal(event)
		key := []byte(randStringBytes(8))
		record := &kgo.Record{Topic: "dimuser", Value: eventValue, Key: key}
		s.client.Produce(s.ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Logger().Errorf("record had a produce error: %s", err)
			}
		})
	}
	return nil
}

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (s *KafkaSink) Send(event map[string]any, stream string, timeCol string) error {
	go s.send(event, stream, timeCol)
	return nil
}

func (s *KafkaSink) send(event map[string]any, stream string, timeCol string) error {
	// TODO : put event to an internal event queue
	log.Logger().Debugf("got one car event here %v", event)
	eventValue, _ := json.Marshal(event)
	key := []byte(randStringBytes(8))
	record := &kgo.Record{Topic: stream, Value: eventValue, Key: key}
	s.client.Produce(s.ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Logger().Errorf("record had a produce error: %s", err)
		}
	})
	return nil
}
