package kdb

import (
	"fmt"
	"sync"

	kdb "github.com/sv/kdbgo"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const KDB_OB_TYPE = "kdb"

type KDBObserver struct {
	host string
	port int

	client *kdb.KDBConn

	metric         string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager metrics.Metrics
}

func NewKDBObserver(properties map[string]interface{}) (observer.Observer, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "port", 5001)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	client, err := kdb.DialKDB(host, port, "")
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	var metricsManager metrics.Metrics
	if _, ok := properties["metric_store_address"]; !ok {
		metricsManager = metrics.NewCSVMetricManager()
	} else {
		metricStoreAddress, err := utils.GetWithDefault(properties, "metric_store_address", "http://localhost:8000")
		if err != nil {
			return nil, fmt.Errorf("invalid properties : %w", err)
		}

		metricStoreAPIKey, err := utils.GetWithDefault(properties, "metric_store_apikey", "")
		if err != nil {
			return nil, fmt.Errorf("invalid properties : %w", err)
		}

		metricStoreTenant, err := utils.GetWithDefault(properties, "metric_store_tenant", "")
		if err != nil {
			return nil, fmt.Errorf("invalid properties : %w", err)
		}

		metricsManager = metrics.NewTimeplusMetricManager(metricStoreAddress, metricStoreTenant, metricStoreAPIKey)
	}

	return &KDBObserver{
		host:           host,
		port:           port,
		client:         client,
		metric:         metric,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metricsManager,
	}, nil
}

func (o *KDBObserver) observeLatency() error {
	return nil
}

func (o *KDBObserver) observeThroughput() error {
	return nil
}

func (o *KDBObserver) observeAvailability() error {
	return nil
}

func (o *KDBObserver) Observe() error {
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

func (o *KDBObserver) Stop() {
}

func (o *KDBObserver) Wait() {
}
