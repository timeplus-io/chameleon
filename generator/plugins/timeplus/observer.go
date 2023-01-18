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

	"github.com/timeplus-io/go-client/timeplus"

	"github.com/google/uuid"
)

const TimeplusOBType = "timeplus"

type TimeplusObserver struct {
	server     *timeplus.TimeplusClient
	query      string
	timeColumn string
	timeFormat string
	metric     string

	querySet []interface{}

	isStopped      bool
	cancel         rxgo.Disposable
	obWaiter       sync.WaitGroup
	metricsManager metrics.Metrics
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

	tenant, err := utils.GetWithDefault(properties, "tenant", "")
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

	ob := &TimeplusObserver{
		server:         timeplus.NewCient(address, tenant, apikey),
		query:          query,
		timeColumn:     timeColumn,
		timeFormat:     timeFormat,
		metric:         metric,
		querySet:       nil,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metricsManager,
	}

	if value, ok := properties["querys"]; ok {
		ob.querySet = value.([]interface{})
	}

	return ob, nil
}

func (o *TimeplusObserver) observeLatency() error {
	log.Logger().Infof("start observing latecny")
	o.metricsManager.Add("latency")

	resultStream, info, err := o.server.QueryStream(o.query)
	if err != nil {
		log.Logger().WithError(err).Errorf("failed to run query")
		return err
	}

	o.obWaiter.Add(1)

	timeIndex := -1
	for index, header := range info.Result.Header {
		if header.Name == o.timeColumn {
			timeIndex = index
			fmt.Printf("time index is %d\n", timeIndex)
		}
	}
	disposed := resultStream.ForEach(func(v interface{}) {
		fmt.Printf("event is %v\n", v)
		// event := v.(map[string]interface{})

		// timestamp := event[o.timeColumn].(float64)
		// tm := time.UnixMilli(int64(timestamp))
		// log.Logger().Infof("observe latency %v", time.Until(tm))
		// o.metricsManager.Observe("latency", -float64(time.Until(tm).Microseconds())/1000.0, nil)

		event := v.([]interface{})
		timestamp := event[timeIndex].(float64)
		tm := time.UnixMilli(int64(timestamp))
		log.Logger().Infof("observe latency %v", time.Until(tm))
		o.metricsManager.Observe("latency", -float64(time.Until(tm).Microseconds())/1000.0, nil)

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

	resultStream, _, err := o.server.QueryStream(o.query)
	if err != nil {
		log.Logger().Errorf("failed to run query")
		return err
	}

	o.obWaiter.Add(1)
	disposed := resultStream.ForEach(func(v interface{}) {
		event := v.([]interface{})
		count := event[1].(float64) // TODO: make col configurable, now hard code to second fields
		log.Logger().Infof("observe throughput %v", count)
		o.metricsManager.Observe("throughput", count, nil)
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
	log.Logger().Infof("start observing availability")
	o.metricsManager.Add("availability")

	resultStream, _, err := o.server.QueryStream(o.query)
	if err != nil {
		log.Logger().Errorf("failed to run query")
		return err
	}

	o.obWaiter.Add(1)
	disposed := resultStream.ForEach(func(v interface{}) {
		event := v.([]interface{})
		count := event[0].(float64) // TODO: make col configurable, now hard code to second fields
		log.Logger().Infof("observe availability %v", count)
		o.metricsManager.Observe("availability", count, nil)
	}, func(err error) {
		log.Logger().Error("query failed", err)
	}, func() {
		log.Logger().Debugf("query %s closed")
	})

	_, cancel := resultStream.Connect(context.Background())
	o.cancel = cancel
	<-disposed
	log.Logger().Infof("stop observing availability")
	o.obWaiter.Done()
	return nil
}

func (o *TimeplusObserver) runQuery(sql string) error {
	o.obWaiter.Add(1)
	defer o.obWaiter.Done()

	id := uuid.NewString() // TODO : the API should return query Id
	metricsName := "query"
	resultStream, _, err := o.server.QueryStream(sql)
	if err != nil {
		log.Logger().Errorf("failed to run query")
		tag := map[string]interface{}{
			"event": "error",
			"query": sql,
			"error": err.Error(),
			"id":    id,
		}
		o.metricsManager.Observe(metricsName, 1, tag)
		return err
	}

	tag := map[string]interface{}{
		"event": "start",
		"query": sql,
		"id":    id,
	}
	o.metricsManager.Observe(metricsName, 1, tag)

	disposed := resultStream.ForEach(func(v interface{}) {
		event := v.(map[string]interface{})
		log.Logger().Infof("observe queries %v", event) // TODO: make col configurable
		tag := map[string]interface{}{
			"event": "data",
			"id":    id,
		}
		o.metricsManager.Observe(metricsName, 1, tag)
	}, func(err error) {
		log.Logger().Error("query failed", err)
		tag := map[string]interface{}{
			"event": "runtime_error",
			"error": err.Error(),
			"id":    id,
		}
		o.metricsManager.Observe(metricsName, 1, tag)
	}, func() {
		tag := map[string]interface{}{
			"event": "close",
			"id":    id,
		}
		o.metricsManager.Observe(metricsName, 1, tag)
		log.Logger().Infof("query %s closed", id)
	})

	_, cancel := resultStream.Connect(context.Background())
	o.cancel = cancel
	<-disposed

	time.Sleep(100 * time.Millisecond)
	o.metricsManager.Flush()
	return nil
}

func (o *TimeplusObserver) observeQueries() error {
	metricsName := "query"
	log.Logger().Info("start observing queries")
	o.metricsManager.Add(metricsName)
	log.Logger().Info("timeplus ob running query")

	for _, query := range o.querySet {
		go func(sql string) {
			o.runQuery(sql)
		}(query.(string))
	}

	o.obWaiter.Wait()
	log.Logger().Infof("stop observing queires")
	return nil
}

func (o *TimeplusObserver) Observe() error {
	log.Logger().Infof("TimeplusObserver start observing")

	if o.metric == "latency" {
		go o.observeLatency()
	} else if o.metric == "throughput" {
		go o.observeThroughput()
	} else if o.metric == "availability" {
		go o.observeAvailability()
	} else if o.metric == "queries" {
		go o.observeQueries()
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
