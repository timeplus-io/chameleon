package kafka

import (
	"fmt"
	"sync"

	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const KAFKA_OB_TYPE = "kafka"

type KafkaObserver struct {
	metric         string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

type KafkaResult map[string]interface{}

func NewKafkaObserver(properties map[string]interface{}) (observer.Observer, error) {
	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &KafkaObserver{
		metric:         metric,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(),
	}, nil
}

func (o *KafkaObserver) observeLatency() error {
	log.Logger().Infof("start observing latency")
	o.metricsManager.Add("latency")
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
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("Kafka")
}

func (o *KafkaObserver) Wait() {
	o.obWaiter.Wait()
}
