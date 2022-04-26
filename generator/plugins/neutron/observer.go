package neutron

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const NEUTRON_OB_TYPE = "neutron"

type NeutronObserver struct {
	server     *NeutronServer
	query      string
	timeColumn string
	timeFormat string
	metric     string

	isStopped      bool
	cancel         rxgo.Disposable
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

func NewNeutronObserver(properties map[string]interface{}) (observer.Observer, error) {
	address, err := utils.GetWithDefault(properties, "address", "http://localhost:8000")
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

	return &NeutronObserver{
		server:         NewNeutronServer(address),
		query:          query,
		timeColumn:     timeColumn,
		timeFormat:     timeFormat,
		metric:         metric,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(),
	}, nil
}

func (o *NeutronObserver) observeLatency() error {
	log.Logger().Infof("start observing latecny")
	o.metricsManager.Add("latency")

	resultStream, err := o.server.QueryStream(o.query)
	if err != nil {
		log.Logger().Errorf("failed to run query")
		return err
	}

	o.obWaiter.Add(1)
	disposed := resultStream.ForEach(func(v interface{}) {
		event := v.(common.Event)
		t, err := time.Parse(o.timeFormat, event[o.timeColumn].(string))
		if err != nil {
			log.Logger().Errorf("failed to parse time column", err)
		}
		log.Logger().Infof("observe latency %v", time.Until(t))
		o.metricsManager.Observe("latency", -float64(time.Until(t).Microseconds())/1000.0)
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

func (o *NeutronObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")

	resultStream, err := o.server.QueryStream(o.query)
	if err != nil {
		log.Logger().Errorf("failed to run query")
		return err
	}

	o.obWaiter.Add(1)
	disposed := resultStream.ForEach(func(v interface{}) {
		event := v.(common.Event)
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

func (o *NeutronObserver) observeAvailability() error {
	log.Logger().Infof("start observing availability")
	o.metricsManager.Add("availability")
	o.obWaiter.Add(1)

	for {
		if o.isStopped {
			log.Logger().Infof("stop neutron availability observing")
			break
		}

		result, err := o.server.SyncQuery(o.query)
		if err != nil {
			log.Logger().Errorf("failed to run query : %w", err)
			continue
		}

		count := result.Data[0][0]
		log.Logger().Infof("observing availability with count = %v", count)
		o.metricsManager.Observe("availability", count.(float64))
		time.Sleep(1 * time.Second)
	}

	log.Logger().Infof("stop observing availability")
	o.obWaiter.Done()
	return nil
}

func (o *NeutronObserver) Observe() error {
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

func (o *NeutronObserver) Stop() {
	log.Logger().Infof("call neutron stop observing")
	o.isStopped = true
	log.Logger().Infof("set stopped")
	o.cancel()
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("neutron")
}

func (o *NeutronObserver) Wait() {
	o.obWaiter.Wait()
}
