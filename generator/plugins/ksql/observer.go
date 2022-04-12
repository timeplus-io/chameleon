package ksql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rmoff/ksqldb-go"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const KSQL_OB_TYPE = "ksql"

type KSQLObserver struct {
	host       string
	port       int
	query      string
	timeFormat string
	client     *ksqldb.Client

	metric         string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

type KSQLResult map[string]interface{}

func NewKSQLObserver(properties map[string]interface{}) (observer.Observer, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "port", 8088)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	query, err := utils.GetWithDefault(properties, "query", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeFormat, err := utils.GetWithDefault(properties, "time_format", "2006-01-02 15:04:05.000000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	url := fmt.Sprintf("http://%s:%d", host, port)
	client := ksqldb.NewClient(url, "", "")

	return &KSQLObserver{
		host:           host,
		port:           port,
		timeFormat:     timeFormat,
		client:         client,
		metric:         metric,
		query:          query,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(),
	}, nil
}

func (o *KSQLObserver) observeLatency() error {
	log.Logger().Infof("start observing latency")
	o.metricsManager.Add("latency")
	o.obWaiter.Add(1)

	rc := make(chan ksqldb.Row)
	hc := make(chan ksqldb.Header, 1)
	k := fmt.Sprintf("%s EMIT CHANGES;", o.query)

	// This Go routine will handle rows as and when they
	// are sent to the channel
	go func() {
		var TIME string
		var VALUE float64
		for row := range rc {
			if o.isStopped {
				break
			}
			if row != nil {
				TIME = row[0].(string)
				VALUE = row[1].(float64)
				log.Logger().Infof("get one row🐾%v: %v\n", TIME, VALUE)
				t, err := time.Parse(o.timeFormat, TIME)
				if err != nil {
					continue
				}
				log.Logger().Infof("observe latency %v", time.Until(t))
				o.metricsManager.Observe("latency", -float64(time.Until(t).Microseconds())/1000.0)
			}
		}
		o.obWaiter.Done()
	}()

	ctx := context.Background()
	e := o.client.Push(ctx, k, rc, hc)
	if e != nil {
		// handle the error better here, e.g. check for no rows returned
		return fmt.Errorf("Error running Push request against ksqlDB:\n%v", e)
	}
	return nil
}

func (o *KSQLObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")
	o.obWaiter.Add(1)

	rc := make(chan ksqldb.Row)
	hc := make(chan ksqldb.Header, 1)
	k := fmt.Sprintf("%s EMIT CHANGES;", o.query)

	// This Go routine will handle rows as and when they
	// are sent to the channel
	preCount := float64(0)
	preTime := time.Now()
	go func() {
		var count float64
		var group string
		for row := range rc {
			if o.isStopped {
				break
			}
			if row != nil {
				count = row[0].(float64)
				group = row[1].(string)
				log.Logger().Debugf("get one row🐾%v: %v", count, group)
				now := time.Now()
				throuput := (count - preCount) / float64(now.Sub(preTime).Seconds())
				log.Logger().Infof("get throughput🐾%v", throuput)
				if preCount > 0 {
					o.metricsManager.Observe("throughput", throuput)
				}
				preCount = count
				preTime = now
			}
		}
		o.obWaiter.Done()
	}()

	ctx := context.Background()
	e := o.client.Push(ctx, k, rc, hc)
	if e != nil {
		// handle the error better here, e.g. check for no rows returned
		return fmt.Errorf("Error running Push request against ksqlDB:\n%v", e)
	}
	return nil
}

func (o *KSQLObserver) observeAvailability() error {
	log.Logger().Infof("start observing availability")
	o.metricsManager.Add("availability")
	return nil
}

func (o *KSQLObserver) Observe() error {
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

func (o *KSQLObserver) Stop() {
	log.Logger().Infof("call ksql stop observing")
	o.isStopped = true
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("ksql")
}

func (o *KSQLObserver) Wait() {
	o.obWaiter.Wait()
}
