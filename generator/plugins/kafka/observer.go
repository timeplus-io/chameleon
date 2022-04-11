package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
)

const KAFKA_OB_TYPE = "kafka"

type KafkaObserver struct {
	brokers       []string
	topic         string
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

	topic, err := utils.GetWithDefault(properties, "topic", "test")
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

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, err
	}

	return &KafkaObserver{
		brokers:        strings.Split(brokers, ","),
		topic:          topic,
		consumerGroup:  consumerGroup,
		metric:         metric,
		valueHit:       valueHit,
		timeColumn:     timeColumn,
		timeFormat:     timeFormat,
		client:         client,
		ctx:            context.Background(),
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(),
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

	return nil
}

func (o *KafkaObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")
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
