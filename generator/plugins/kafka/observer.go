package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const KAFKA_OB_TYPE = "kafka"

type KafkaObserver struct {
	brokers       []string
	topic         string
	tls           bool
	sasl          string
	saslUsername  string
	saslPassword  string
	consumerGroup string
	valueHit      int
	timeColumn    string
	timeFormat    string
	client        *kgo.Client
	ctx           context.Context

	metric         string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

type KafkaResult map[string]interface{}

func NewKafkaObserver(properties map[string]interface{}) (observer.Observer, error) {
	brokers, err := utils.GetWithDefault(properties, "brokers", "localhost:9092")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	enableTls, err := utils.GetBoolWithDefault(properties, "tls", false)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	topic, err := utils.GetWithDefault(properties, "topic", "test")
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

	consumerGroup := fmt.Sprintf("my-group-%s", uuid.Must(uuid.NewRandom()).String())

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	valueHit, err := utils.GetIntWithDefault(properties, "value_hit", 9)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeColumn, err := utils.GetWithDefault(properties, "time_column", "time")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeFormat, err := utils.GetWithDefault(properties, "time_format", "2006-01-02 15:04:05.000000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.ConsumeTopics(topic),
	}

	if enableTls {
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	if sasl == KAFKA_SASL_TYPE_PLAIN {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: saslUsername,
			Pass: saslPassword,
		}.AsMechanism()))
	} else if sasl == KAFKA_SASL_TYPE_SCRAM {
		opts = append(opts, kgo.SASL(scram.Auth{
			User: saslUsername,
			Pass: saslPassword,
		}.AsSha512Mechanism()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	metricStoreAddress, err := utils.GetWithDefault(properties, "metric_store_address", "http://localhost:8000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metricStoreAPIKey, err := utils.GetWithDefault(properties, "metric_store_apikey", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &KafkaObserver{
		brokers:        strings.Split(brokers, ","),
		topic:          topic,
		sasl:           sasl,
		saslUsername:   saslUsername,
		saslPassword:   saslPassword,
		consumerGroup:  consumerGroup,
		metric:         metric,
		valueHit:       valueHit,
		timeColumn:     timeColumn,
		timeFormat:     timeFormat,
		client:         client,
		ctx:            context.Background(),
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(metricStoreAddress, metricStoreAPIKey),
	}, nil
}

func (o *KafkaObserver) deleteGroup() error {
	admClient := kadm.NewClient(o.client)
	admClient.DeleteGroups(o.ctx, o.consumerGroup)
	return nil
}

func (o *KafkaObserver) observeLatency() error {
	log.Logger().Infof("start observing latency")
	o.metricsManager.Add("latency")
	o.obWaiter.Add(1)

	for {
		if o.isStopped {
			log.Logger().Infof("stop kafka latency observing")
			break
		}

		fetches := o.client.PollFetches(o.ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var recordResult map[string]interface{}
			json.NewDecoder(bytes.NewBuffer(record.Value)).Decode(&recordResult)
			log.Logger().Debugf("got one event %v", recordResult)

			if recordResult["value"].(float64) == float64(o.valueHit) {
				log.Logger().Infof("got one hit event %v", recordResult)
				t, err := time.Parse(o.timeFormat, recordResult[o.timeColumn].(string))
				if err != nil {
					log.Logger().Errorf("failed to parse time column", err)
					continue
				}

				log.Logger().Infof("observe latency %v", time.Until(t))
				o.metricsManager.Observe("latency", -float64(time.Until(t).Microseconds())/1000.0)
			}
		}
	}

	o.obWaiter.Done()
	return nil
}

func (o *KafkaObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")
	preOffset := int64(0)
	for {
		if o.isStopped {
			log.Logger().Infof("stop kafka throughput observing")
			break
		}

		admClient := kadm.NewClient(o.client)
		offsets, err := admClient.ListEndOffsets(o.ctx, o.topic)
		if err != nil {
			log.Logger().Warnf("failed to describe groups, %w", err)
			continue
		}

		// TODO: support multiple partitions
		offset := offsets[o.topic][0].Offset
		throughput := offset - int64(preOffset)

		log.Logger().Debugf("the offset is %v", offset)
		log.Logger().Infof("the throughput is %v", throughput)
		if preOffset != 0 {
			o.metricsManager.Observe("throughput", float64(throughput))
		}
		preOffset = offset
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (o *KafkaObserver) observeAvailability() error {
	log.Logger().Infof("start observing availability")
	o.metricsManager.Add("availability")
	return nil
}

func (o *KafkaObserver) Observe() error {
	log.Logger().Infof("start observing")
	if o.metric == "latency" {
		go o.observeLatency()
	}

	if o.metric == "throughput" {
		go o.observeThroughput()
	}

	if o.metric == "availability" {
		go o.observeAvailability()
	}

	return nil
}

func (o *KafkaObserver) Stop() {
	log.Logger().Infof("call Kafka stop observing")
	o.isStopped = true
	o.obWaiter.Wait()
	o.deleteGroup()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("Kafka")
}

func (o *KafkaObserver) Wait() {
	o.obWaiter.Wait()
}
