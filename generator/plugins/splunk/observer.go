package splunk

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const SPLUNK_OB_TYPE = "splunk"

type SplunkObserver struct {
	client *http.Client
	search string
	host   string
	port   int
}

type SplunkResult map[string]interface{}

type SplunkEvents struct {
	Preview bool         `json:"preview"`
	Offset  int          `json:"offset"`
	Result  SplunkResult `json:"result"`
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

	return &SplunkObserver{
		client: utils.NewDefaultHttpClient(),
		search: search,
		host:   host,
		port:   port,
	}, nil
}

func (o *SplunkObserver) Observe() error {
	log.Logger().Infof("start observing")
	splunkUrl := fmt.Sprintf("https://%s:%d/services/search/jobs/export", o.host, o.port)
	searchReq := &url.Values{}
	searchReq.Add("search", `search index=main source="my_source"   value=100 | eval eventtime=_time | eval indextime=_indextime`)
	searchReq.Add("search_mode", "realtime")
	searchReq.Add("earliest_time", "rt")
	searchReq.Add("latest_time", "rt")
	searchReq.Add("output_mode", "json")

	stream, err := HttpRequestStreamWithUser(http.MethodPost, splunkUrl, searchReq, o.client, "admin", "Password!")
	if err != nil {
		return fmt.Errorf("failed to create search : %w", err)
	}

	layout := "2006-01-02 15:04:05.000000" // should be a config

	for item := range stream.Observe() {
		event := item.V.(SplunkEvents)
		raw := event.Result["_raw"]
		var rawEvent map[string]interface{}
		json.NewDecoder(bytes.NewBuffer([]byte(raw.(string)))).Decode(&rawEvent)
		log.Logger().Debugf("get one search result raw : %v ", rawEvent)

		eventTime := rawEvent["time"].(string)
		t, err := time.Parse(layout, eventTime)
		if err != nil {
			continue
		}
		log.Logger().Infof("observe latency %v", time.Until(t))
	}

	log.Logger().Infof("stop observing")
	return nil
}

func (o *SplunkObserver) Stop() {
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
			var event SplunkEvents
			json.NewDecoder(bytes.NewBuffer(text)).Decode(&event)

			streamChannel <- rxgo.Of(event)
			// check stop here
		}
		close(streamChannel)
	}()

	return streamResult, nil
}
