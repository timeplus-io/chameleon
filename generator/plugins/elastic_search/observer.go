package elastic_search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/metrics"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
	"io"
	"io/ioutil"
	"sync"
	"time"
)

const ELASTIC_SEARCH_OB_TYPE = "elastic_search"

type ElasticSearchObserver struct {
	client         *elasticsearch.Client
	search         string
	addresses      []string
	index          string
	metric         string
	timeFormat     string
	timeField      string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager *metrics.Manager
}

func NewElasticSearchObserver(properties map[string]interface{}) (observer.Observer, error) {
	search, err := utils.GetWithDefault(properties, "search", "search *")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	addresses, err := utils.GetStringListWithDefault(properties, "addresses", []string{"http://localhost:9200"})
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	index, err := utils.GetWithDefault(properties, "index", "main")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	metric, err := utils.GetWithDefault(properties, "metric", "latency")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeFormat, err := utils.GetWithDefault(properties, "time_format", "2006-01-02 15:04:05.000000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeField, err := utils.GetWithDefault(properties, "time_field", "time")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	cfg := elasticsearch.Config{
		Addresses: addresses,
	}
	client, err := elasticsearch.NewClient(cfg)

	return &ElasticSearchObserver{
		client:         client,
		search:         search,
		index:          index,
		metric:         metric,
		timeFormat:     timeFormat,
		timeField:      timeField,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metrics.NewManager(),
	}, nil
}

func (o *ElasticSearchObserver) observeLatency() error {
	log.Logger().Infof("start observing latency")
	o.metricsManager.Add("latency")
	stream, err := ElasticSearchRequestStream(o.search, o.index, o.client)
	if err != nil {
		return fmt.Errorf("failed to create search : %w", err)
	}

	o.obWaiter.Add(1)
	for item := range stream.Observe() {
		if o.isStopped {
			log.Logger().Infof("stop splunk observing")
			break
		}
		event := item.V.(map[string]interface{})
		raw := event["_source"]
		rawSource := raw.(map[string]interface{})["event"]

		t, err := time.Parse(o.timeFormat, rawSource.(map[string]interface{})["time"].(string))
		if err != nil {
			continue
		}
		log.Logger().Infof("observe latency %v", time.Until(t))
		o.metricsManager.Observe("latency", -float64(time.Until(t).Microseconds())/1000.0)
	}
	o.obWaiter.Done()
	return nil
}

func (o *ElasticSearchObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")
	stream, err := ElasticSearchRequestStream(o.search, o.index, o.client)
	if err != nil {
		return fmt.Errorf("failed to create search : %w", err)
	}

	o.obWaiter.Add(1)
	for item := range stream.Observe() {
		if o.isStopped {
			log.Logger().Infof("stop splunk observing")
			break
		}
		event := item.V.(map[string]interface{})
		count := len(event)
		log.Logger().Debugf("get one search result count : %v ", count)
		o.metricsManager.Observe("throughput", float64(count))

	}
	o.obWaiter.Done()
	return nil
}

func (o *ElasticSearchObserver) Observe() error {
	log.Logger().Infof("start observing")
	if o.metric == "latency" {
		go o.observeLatency()
	}

	if o.metric == "throughput" {
		go o.observeThroughput()
	}

	return nil
}

func (o *ElasticSearchObserver) Stop() {
	log.Logger().Infof("call splunk stop observing")
	o.isStopped = true
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("splunk")
}

func ElasticSearchRequestStream(search string, index string, client *elasticsearch.Client) (rxgo.Observable, error) {

	log.Logger().Infof("observe elastic search query %v", search)

	streamChannel := make(chan rxgo.Item)
	streamResult := rxgo.FromChannel(streamChannel)

	go func() {
		for {
			var r map[string]interface{}
			res, err := client.Search(
				client.Search.WithContext(context.Background()),
				client.Search.WithIndex(index),
				client.Search.WithBody(bytes.NewBufferString(search)),
				client.Search.WithTrackTotalHits(true),
				client.Search.WithPretty(),
			)
			if err != nil {
				log.Logger().Fatalf("Error getting response: %s", err)
			}

			if res.IsError() {
				var e map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
					log.Logger().Fatalf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and error information.
					log.Logger().Fatalf("[%s] %s: %s",
						res.Status(),
						e["error"].(map[string]interface{})["type"],
						e["error"].(map[string]interface{})["reason"],
					)
				}
			}

			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				log.Logger().Fatalf("Error parsing the response body: %s", err)
			}
			res.Body.Close()
			io.Copy(ioutil.Discard, res.Body)
			for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
				streamChannel <- rxgo.Of(hit)
			}
		}
		log.Logger().Infof("No more elastic search event")
		close(streamChannel)
	}()

	return streamResult, nil
}
