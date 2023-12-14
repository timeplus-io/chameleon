package splunk

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/metrics"
	"github.com/timeplus-io/chameleon/generator/internal/observer"
	"github.com/timeplus-io/chameleon/generator/internal/utils"
)

const SPLUNK_OB_TYPE = "splunk"

type SplunkObserver struct {
	client         *http.Client
	search         string
	host           string
	port           int
	username       string
	password       string
	metric         string
	timeFormat     string
	timeField      string
	isStopped      bool
	obWaiter       sync.WaitGroup
	metricsManager metrics.Metrics
}

type SplunkResult map[string]interface{}

type SplunkEvents struct {
	Preview bool         `json:"preview,omitempty"`
	Offset  int          `json:"offset,omitempty"`
	Result  SplunkResult `json:"result,omitempty"`
	Lastrow bool         `json:"lastrow,omitempty"`
}

func NewSplunkObserver(properties map[string]interface{}) (observer.Observer, error) {
	search, err := utils.GetWithDefault(properties, "search", "search *")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "host", 8089)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	username, err := utils.GetWithDefault(properties, "username", "admin")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	password, err := utils.GetWithDefault(properties, "password", "Password!")
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

	var metricsManager metrics.Metrics
	if _, ok := properties["metric_store_address"]; !ok {
		metricsManager = metrics.NewEmptyMetricManager()
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

	httpClient := utils.NewDefaultHttpClient()
	httpClient.Timeout = 60 * 60 * time.Second

	return &SplunkObserver{
		client:         httpClient,
		search:         search,
		host:           host,
		port:           port,
		username:       username,
		password:       password,
		metric:         metric,
		timeFormat:     timeFormat,
		timeField:      timeField,
		isStopped:      false,
		obWaiter:       sync.WaitGroup{},
		metricsManager: metricsManager,
	}, nil
}

func (o *SplunkObserver) observeLatency() error {
	log.Logger().Infof("start observing latency")
	o.metricsManager.Add("latency")
	splunkUrl := fmt.Sprintf("https://%s:%d/services/search/jobs/export", o.host, o.port)
	searchReq := &url.Values{}
	searchReq.Add("search", o.search)
	searchReq.Add("search_mode", "realtime")
	searchReq.Add("earliest_time", "rt")
	searchReq.Add("latest_time", "rt")
	searchReq.Add("output_mode", "json")
	searchReq.Add("auto_cancel", "0")
	searchReq.Add("auto_finalize_ec", "0")
	searchReq.Add("max_time", "0")

	log.Logger().Infof("observe splunk search %v", searchReq)
	stream, err := HttpRequestStreamWithUser(http.MethodPost, splunkUrl, searchReq, o.client, o.username, o.password)
	if err != nil {
		return fmt.Errorf("failed to create search : %w", err)
	}

	o.obWaiter.Add(1)
	for item := range stream.Observe() {
		if o.isStopped {
			log.Logger().Infof("stop splunk observing")
			break
		}
		event := item.V.(SplunkEvents)
		raw := event.Result["_raw"]
		var rawEvent map[string]interface{}
		json.NewDecoder(bytes.NewBuffer([]byte(raw.(string)))).Decode(&rawEvent)
		log.Logger().Debugf("get one search result raw : %v ", rawEvent)

		eventTime := rawEvent[o.timeField].(string)
		t, err := time.Parse(o.timeFormat, eventTime)
		if err != nil {
			continue
		}
		log.Logger().Infof("observe latency %v", time.Until(t))
		o.metricsManager.Observe("latency", -float64(time.Until(t).Microseconds())/1000.0, nil)
	}
	o.obWaiter.Done()
	return nil
}

func (o *SplunkObserver) observeThroughput() error {
	log.Logger().Infof("start observing throughput")
	o.metricsManager.Add("throughput")
	splunkUrl := fmt.Sprintf("https://%s:%d/services/search/jobs/export", o.host, o.port)
	searchReq := &url.Values{}
	searchReq.Add("search", o.search)
	searchReq.Add("search_mode", "realtime")
	searchReq.Add("earliest_time", "rt-10s")
	searchReq.Add("latest_time", "rt")
	searchReq.Add("output_mode", "json")
	searchReq.Add("auto_cancel", "0")
	searchReq.Add("auto_finalize_ec", "0")
	searchReq.Add("max_time", "0")

	log.Logger().Infof("observe splunk search %v", searchReq)
	time.Sleep(3 * time.Second)
	log.Logger().Infof("waited 3 seconds")

	stream, err := HttpRequestStreamWithUser(http.MethodPost, splunkUrl, searchReq, o.client, o.username, o.password)
	if err != nil {
		return fmt.Errorf("failed to create search : %w", err)
	}

	o.obWaiter.Add(1)
	for item := range stream.Observe() {
		if o.isStopped {
			log.Logger().Infof("stop splunk observing")
			break
		}
		event := item.V.(SplunkEvents)
		if event.Result["count"] != nil {
			count := event.Result["count"].(string)
			log.Logger().Debugf("get one search result count : %v ", count)
			if s, err := strconv.ParseFloat(count, 64); err == nil {
				log.Logger().Infof("observe throughput %f", s)
				o.metricsManager.Observe("throughput", s, nil)
			}
		}
	}
	o.obWaiter.Done()
	return nil
}

func (o *SplunkObserver) observeAvailability() error {
	log.Logger().Infof("start observing availability")
	o.metricsManager.Add("availability")
	splunkUrl := fmt.Sprintf("https://%s:%d/services/search/jobs/export", o.host, o.port)
	searchReq := &url.Values{}
	searchReq.Add("search", o.search)
	searchReq.Add("search_mode", "realtime")
	searchReq.Add("earliest_time", "rt-1h")
	searchReq.Add("latest_time", "rt")
	searchReq.Add("output_mode", "json")
	searchReq.Add("auto_cancel", "0")
	searchReq.Add("auto_finalize_ec", "0")
	searchReq.Add("max_time", "0")

	log.Logger().Infof("observe splunk availability search %v", searchReq)
	stream, err := HttpRequestStreamWithUser(http.MethodPost, splunkUrl, searchReq, o.client, o.username, o.password)
	if err != nil {
		return fmt.Errorf("failed to create search : %w", err)
	}

	o.obWaiter.Add(1)
	for item := range stream.Observe() {
		if o.isStopped {
			log.Logger().Infof("stop splunk availability observing")
			break
		}
		event := item.V.(SplunkEvents)
		count := event.Result["count"].(string)
		log.Logger().Debugf("get one search result count : %v ", count)
		if s, err := strconv.ParseFloat(count, 64); err == nil {
			log.Logger().Infof("observe availability %f", s)
			o.metricsManager.Observe("availability", s, nil)
		}
	}
	o.obWaiter.Done()
	return nil
}

func (o *SplunkObserver) Observe() error {
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

func (o *SplunkObserver) Stop() {
	log.Logger().Infof("call splunk stop observing")
	o.isStopped = true
	o.obWaiter.Wait()
	log.Logger().Infof("stop observing")
	o.metricsManager.Save("splunk")
}

func (o *SplunkObserver) Wait() {
	o.obWaiter.Wait()
}

func HttpRequestStreamWithUser(method string, url string, payload *url.Values, client *http.Client, username string, password string) (rxgo.Observable, error) {
	// note: this is specific for splunk search
	var body io.Reader
	if payload != nil {
		body = bytes.NewBufferString(payload.Encode())
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(res.Body)
	streamChannel := make(chan rxgo.Item)
	streamResult := rxgo.FromChannel(streamChannel)
	go func() {
		defer res.Body.Close()
		for scanner.Scan() {
			text := []byte(scanner.Text())
			log.Logger().Debugf("the returned splunk result is %v", string(text))

			var event SplunkEvents
			json.NewDecoder(bytes.NewBuffer(text)).Decode(&event)

			streamChannel <- rxgo.Of(event)
			// check stop here
		}
		log.Logger().Infof("No more splunk stream event")
		close(streamChannel)
	}()

	return streamResult, nil
}
