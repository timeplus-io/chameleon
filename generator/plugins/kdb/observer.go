package kdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	kdb "github.com/sv/kdbgo"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const KDB_OB_TYPE = "kdb"

type KDBObserver struct {
	client *kdb.KDBConn
	query  string

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

	query, err := utils.GetWithDefault(properties, "query", "count test")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
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
		client:         client,
		query:          query,
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
	log.Logger().Infof("availability observing started")
	o.metricsManager.Add("availability")

	o.obWaiter.Add(1)
	defer o.obWaiter.Done()

	metricsName := "availability"
	var preCount int64 = 0
	id, _ := uuid.NewRandom()
	tag := map[string]interface{}{"targte": "kdb", "testId": id.String()}

	for {
		res, err := o.client.Call(o.query)
		if err != nil {
			log.Logger().Errorf("query kdb failed: %w", err)
		} else {
			count := res.Data.(int64)
			log.Logger().Infof("query success result: %d", count)
			o.metricsManager.Observe(metricsName, float64(count), tag)
			if preCount != 0 && count == preCount {
				break
			} else {
				preCount = count
			}
		}
		time.Sleep(2 * time.Second)
	}

	time.Sleep(100 * time.Millisecond)
	o.metricsManager.Flush()

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
	log.Logger().Infof("call kdb stop observing")
	o.isStopped = true
	o.obWaiter.Wait()
	o.metricsManager.Save("kdb")
	log.Logger().Infof("save observing completed")
}

func (o *KDBObserver) Wait() {
	o.obWaiter.Wait()
}
