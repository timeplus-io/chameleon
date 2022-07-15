package timeplus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"

	timeplus "github.com/timeplus-io/go-client/client"
)

const TimeplusOBType = "timeplus"

type TimeplusObserver struct {
	server     *timeplus.TimeplusClient
	query      string
	timeColumn string
	timeFormat string
	metric     string

	isStopped      bool
	cancel         rxgo.Disposable
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

func NewTimeplusObserver(properties map[string]interface{}) (observer.Observer, error) {
	address, err := utils.GetWithDefault(properties, "address", "http://localhost:8000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	apikey, err := utils.GetWithDefault(properties, "apikey", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	query, err := utils.GetWithDefault(properties, "query", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeColumn, err := utils.GetWithDefault(properties, "time_column", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeFormat, err := utils.GetWithDefault(properties, "time_format", "2006-01-02T15:04:05.000Z")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metricStoreAddress, err := utils.GetWithDefault(properties, "metric_store_address", "http://localhost:8000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metricStoreAPIKey, err := utils.GetWithDefault(properties, "metric_store_apikey", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &TimeplusObserver{
		server:         timeplus.NewCient(address, apikey),
		query:          query,
		timeColumn:     timeColumn,
		timeFormat:     timeFormat,
		metric:         metric,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(metricStoreAddress, metricStoreAPIKey),
	}, nil
}

func (o *TimeplusObserver) observeLatency() error {
	log.Logger().Infof("start observing latecny")
	o.metricsManager.Add("latency")

	resultStream, err := o.server.QueryStream(o.query)
	if err != nil {
		log.Logger().WithError(err).Errorf("failed to run query")
		return err
	}

	o.obWaiter.Add(1)
	disposed := resultStream.ForEach(func(v interface{}) {
		event := v.(map[string]interface{})

		timestamp := event[o.timeColumn].(float64)
		tm := time.UnixMilli(int64(timestamp))
		log.Logger().Infof("observe latency %v", time.Until(tm))
		o.metricsManager.Observe("latency", -float64(time.Until(tm).Microseconds())/1000.0)

	}, func(err error) {
		log.Logger().Error("query failed", err)
	}, func() {
		log.Logger().Debugf("query %s closed")
	})

	_, cancel := resultStream.Connect(context.Background())
	o.cancel = cancel
	<-disposed
	log.Logger().Infof("stop observing latecny")
	o.obWaiter.Done()
	return nil
}

func (o *TimeplusObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")

	resultStream, err := o.server.QueryStream(o.query)
	if err != nil {
		log.Logger().Errorf("failed to run query")
		return err
	}

	o.obWaiter.Add(1)
	disposed := resultStream.ForEach(func(v interface{}) {
		event := v.(map[string]interface{})
		log.Logger().Infof("observe throughput %v", event["count"]) // TODO: make col configurable
		o.metricsManager.Observe("throughput", event["count"].(float64))
	}, func(err error) {
		log.Logger().Error("query failed", err)
	}, func() {
		log.Logger().Debugf("query %s closed")
	})

	_, cancel := resultStream.Connect(context.Background())
	o.cancel = cancel
	<-disposed
	log.Logger().Infof("stop observing throughput")
	o.obWaiter.Done()
	return nil
}

func (o *TimeplusObserver) observeAvailability() error {
	return fmt.Errorf("availability not supported")
	// log.Logger().Infof("start observing availability")
	// o.metricsManager.Add("availability")
	// o.obWaiter.Add(1)

	// for {
	// 	if o.isStopped {
	// 		log.Logger().Infof("stop timeplus availability observing")
	// 		break
	// 	}

	// 	result, err := o.server.SyncQuery(o.query)
	// 	if err != nil {
	// 		log.Logger().Errorf("failed to run query : %w", err)
	// 		continue
	// 	}

	// 	count := result.Data[0][0]
	// 	log.Logger().Infof("observing availability with count = %v", count)
	// 	o.metricsManager.Observe("availability", count.(float64))
	// 	time.Sleep(1 * time.Second)
	// }

	// log.Logger().Infof("stop observing availability")
	// o.obWaiter.Done()
	// return nil
}

func (o *TimeplusObserver) Observe() error {
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

func (o *TimeplusObserver) Stop() {
	log.Logger().Infof("call timeplus stop observing")
	o.isStopped = true
	log.Logger().Infof("set stopped")
	o.cancel() // a bug with cancel when timeout
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("timeplus")
}

func (o *TimeplusObserver) Wait() {
	o.obWaiter.Wait()
}
