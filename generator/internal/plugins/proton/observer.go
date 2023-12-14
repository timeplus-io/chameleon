package proton

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/metrics"
	"github.com/timeplus-io/chameleon/generator/internal/observer"
	"github.com/timeplus-io/chameleon/generator/internal/utils"
	"github.com/timeplus-io/go-client/timeplus"

	"github.com/google/uuid"
)

const ProtonOBType = "proton"

type ProtonObserver struct {
	server     *Engine
	query      string
	timeColumn string
	timeFormat string
	metric     string

	bufferCount int
	bufferTime  int

	querySet []interface{}

	isStopped      bool
	cancel         func()
	obWaiter       sync.WaitGroup
	metricsManager metrics.Metrics
}

func NewProtonObserver(properties map[string]interface{}) (observer.Observer, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	username, err := utils.GetWithDefault(properties, "username", "default")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	password, err := utils.GetWithDefault(properties, "password", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	config := NewConfig(host, username, password)
	engine := NewEngine(config)

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

	bufferCount, err := utils.GetIntWithDefault(properties, "buffer_count", 100)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	bufferTime, err := utils.GetIntWithDefault(properties, "buffer_time", 128)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metricsManager := metrics.NewCSVMetricManager()

	ob := &ProtonObserver{
		server:         engine,
		query:          query,
		timeColumn:     timeColumn,
		timeFormat:     timeFormat,
		metric:         metric,
		querySet:       nil,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metricsManager,
		bufferCount:    bufferCount,
		bufferTime:     bufferTime,
	}

	if value, ok := properties["querys"]; ok {
		ob.querySet = value.([]interface{})
	}

	return ob, nil
}

func (o *ProtonObserver) observeLatency() error {
	log.Logger().Infof("start observing latecny")
	o.metricsManager.Add("latency")

	id := uuid.NewString()
	ctx, cancel := context.WithCancel(context.Background())
	header, resultStream, _, err := o.server.QueryStream(ctx, o.query, id)
	if err != nil {
		log.Logger().WithError(err).Errorf("failed to run query")
		cancel()
		return err
	}

	o.obWaiter.Add(1)

	timeIndex := -1
	for index, header := range header {
		if header.Name == o.timeColumn {
			timeIndex = index
			fmt.Printf("time index is %d\n", timeIndex)
		}
	}
	resultStream.ForEach(func(v interface{}) {
		fmt.Printf("event is %v\n", v)

		event := v.(*timeplus.DataEvent)
		for _, e := range *event {
			timestamp := e[timeIndex].(float64)
			tm := time.UnixMilli(int64(timestamp))
			log.Logger().Infof("observe latency %v", time.Until(tm))
			o.metricsManager.Observe("latency", -float64(time.Until(tm).Microseconds())/1000.0, nil)
		}

	}, func(err error) {
		log.Logger().Error("query failed", err)
	}, func() {
		log.Logger().Debugf("query %s closed", o.query)
	})

	resultStream.Connect(ctx)
	o.cancel = cancel
	log.Logger().Infof("stop observing latecny")
	o.obWaiter.Done()
	return nil
}

func (o *ProtonObserver) observeThroughput() error {
	// log.Logger().Infof("start observing throughput")
	// o.metricsManager.Add("throughput")

	// resultStream, cancel, _, err := o.server.QueryStream(o.query, o.bufferCount, o.bufferTime)
	// if err != nil {
	// 	log.Logger().Errorf("failed to run query")
	// 	return err
	// }

	// o.obWaiter.Add(1)
	// disposed := resultStream.ForEach(func(v interface{}) {
	// 	event := v.(*timeplus.DataEvent)
	// 	for _, e := range *event {
	// 		count := e[1].(float64) // TODO: make col configurable, now hard code to second fields
	// 		log.Logger().Infof("observe throughput %v", count)
	// 		o.metricsManager.Observe("throughput", count, nil)
	// 	}
	// }, func(err error) {
	// 	log.Logger().Error("query failed", err)
	// }, func() {
	// 	log.Logger().Debugf("query %s closed")
	// })

	// o.cancel = cancel
	// <-disposed
	// log.Logger().Infof("stop observing throughput")
	// o.obWaiter.Done()
	return nil
}

func (o *ProtonObserver) observeAvailability() error {
	return fmt.Errorf("not implemented")
}

func (o *ProtonObserver) runQuery(sql string) error {
	o.obWaiter.Add(1)
	defer o.obWaiter.Done()

	id := uuid.NewString() // TODO : the API should return query Id
	metricsName := "query"
	ctx, cancel := context.WithCancel(context.Background())
	_, resultStream, _, err := o.server.QueryStream(ctx, sql, id)
	if err != nil {
		log.Logger().Errorf("failed to run query")
		tag := map[string]interface{}{
			"event": "error",
			"query": sql,
			"error": err.Error(),
			"id":    id,
		}
		o.metricsManager.Observe(metricsName, 1, tag)
		cancel()
		return err
	}

	tag := map[string]interface{}{
		"event": "start",
		"query": sql,
		"id":    id,
	}
	o.metricsManager.Observe(metricsName, 1, tag)

	resultStream.ForEach(func(v interface{}) {
		event := v.(ResponseDataRow)
		for _, e := range event {
			log.Logger().Infof("observe queries %v", e) // TODO: make col configurable
			tag := map[string]interface{}{
				"event": "data",
				"id":    id,
			}
			o.metricsManager.Observe(metricsName, 1, tag)
		}
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

	resultStream.Connect(ctx)
	o.cancel = cancel

	time.Sleep(100 * time.Millisecond)
	o.metricsManager.Flush()
	return nil
}

func (o *ProtonObserver) observeQueries() error {
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

func (o *ProtonObserver) Observe() error {
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

func (o *ProtonObserver) Stop() {
	log.Logger().Infof("call proton stop observing")
	o.isStopped = true
	log.Logger().Infof("set stopped")
	o.cancel()
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("proton")
}

func (o *ProtonObserver) Wait() {
	o.obWaiter.Wait()
}
