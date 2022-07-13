package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/timeplus/utils"
)

const TimeFormat = "2006-01-02 15:04:05.000"
const APIVersion = "v1beta1"

type ColumnDef struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Default string `json:"default"`
}

type StreamDef struct {
	Name                   string      `json:"name"`
	Columns                []ColumnDef `json:"columns"`
	EventTimeColumn        string      `json:"event_time_column,omitempty"`
	EventTimeZone          string      `json:"event_time_timezone,omitempty"`
	TTLExpression          string      `json:"ttl_expression,omitempty"`
	LogStoreRetentionBytes int         `json:"logstore_retention_bytes,omitempty"`
	LogStoreRetentionMS    int         `json:"logstore_retention_ms,omitempty"`
}

type View struct {
	Name         string `json:"name"`
	Query        string `json:"query"`
	Materialized bool   `json:"materialized,omitempty"`
}

type IngestData struct {
	Columns []string `json:"columns"`
	Data    [][]any  `json:"data"`
}

type IngestPayload struct {
	Data   IngestData `json:"data"`
	Stream string     `json:"stream"`
}

type Query struct {
	SQL         string   `json:"sql"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

type QueryInfo struct {
	ID           string      `json:"id"`
	Name         string      `json:"name"`
	SQL          string      `json:"sql"`
	Description  string      `json:"description"`
	Tags         []string    `json:"tags"`
	Stat         QueryStat   `json:"stat"`
	StartTime    int64       `json:"start_time"`
	EndTime      int64       `json:"end_time"`
	Duration     int64       `json:"duration"`
	ResponseTime int64       `json:"response_time"`
	Status       string      `json:"status"`
	Message      string      `json:"message"`
	Result       QueryResult `json:"result"`
}

type SQLRequest struct {
	SQL     string `json:"sql"`
	Timeout int    `json:"timeout"`
}

type QueryResult struct {
	Header []ColumnDef     `json:"header"`
	Data   [][]interface{} `json:"data"`
}

type QueryStat struct {
	Count      int            `json:"count"`
	Latency    LatencyStat    `json:"latency"`
	Throughput ThroughputStat `json:"throughput"`
}

type LatencyStat struct {
	Min    float64   `json:"min"`
	Max    float64   `json:"max"`
	Sum    float64   `json:"sum"`
	Avg    float64   `json:"avg"`
	Latest []float64 `json:"latest"`
}

type ThroughputStat struct {
	Value float32 `json:"value"`
}

type TimeplusClient struct {
	address string
	apikey  string
	client  *http.Client
}

func NewCient(address string, apikey string) *TimeplusClient {
	return &TimeplusClient{
		address: address,
		apikey:  apikey,
		client:  utils.NewDefaultHttpClient(),
	}
}

func (s *TimeplusClient) CreateStream(streamDef StreamDef) error {
	url := fmt.Sprintf("%s/api/%s/streams", s.address, APIVersion)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, streamDef, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", streamDef.Name, err)
	}
	return nil
}

func (s *TimeplusClient) DeleteStream(streamName string) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s", s.address, APIVersion, streamName)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodDelete, url, nil, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", streamName, err)
	}
	return nil
}

func (s *TimeplusClient) ExistStream(name string) bool {
	streams, err := s.ListStream()
	if err != nil {
		return false
	}

	for _, s := range streams {
		if s.Name == name {
			return true
		}
	}

	return false
}

func (s *TimeplusClient) ListStream() ([]StreamDef, error) {
	url := fmt.Sprintf("%s/api/%s/streams", s.address, APIVersion)
	_, respBody, err := utils.HttpRequestWithAPIKey(http.MethodGet, url, nil, s.client, s.apikey)
	if err != nil {
		return nil, fmt.Errorf("failed to list stream : %w", err)
	}

	var payload []StreamDef
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)

	return payload, nil
}

func (s *TimeplusClient) CreateView(view View) error {
	url := fmt.Sprintf("%s/api/%s/views", s.address, APIVersion)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, view, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to create view %s: %w", view.Name, err)
	}
	return nil
}

func (s *TimeplusClient) ListView() ([]View, error) {
	url := fmt.Sprintf("%s/api/%s/views", s.address, APIVersion)
	_, respBody, err := utils.HttpRequestWithAPIKey(http.MethodGet, url, nil, s.client, s.apikey)
	if err != nil {
		return nil, fmt.Errorf("failed to list views : %w", err)
	}

	var payload []View
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)

	return payload, nil
}

func (s *TimeplusClient) ExistView(name string) bool {
	views, err := s.ListView()
	if err != nil {
		return false
	}

	for _, v := range views {
		if v.Name == name {
			return true
		}
	}

	return false
}

func (s *TimeplusClient) InsertData(data IngestPayload) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s/ingest", s.address, APIVersion, data.Stream)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, data.Data, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to ingest data into stream %s: %w", data.Stream, err)
	}
	return nil
}

func toEvent(headers []ColumnDef, data []interface{}) map[string]interface{} {
	event := make(map[string]interface{})
	for index, header := range headers {
		event[header.Name] = data[index]
	}
	return event
}

func (s *TimeplusClient) QueryStream(sql string) (rxgo.Observable, error) {
	query := Query{
		SQL:         sql,
		Name:        "",
		Description: "",
		Tags:        []string{},
	}

	createQueryUrl := fmt.Sprintf("%s/api/%s/queries", s.address, APIVersion)
	_, respBody, err := utils.HttpRequestWithAPIKey(http.MethodPost, createQueryUrl, query, s.client, s.apikey)
	if err != nil {
		return nil, fmt.Errorf("failed to create query : %w", err)
	}

	var queryResult QueryInfo
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&queryResult)

	wsUrl := fmt.Sprintf("%s/ws/queries/%s", s.address, queryResult.ID)
	wsUrl = strings.Replace(wsUrl, "http", "ws", 1)

	c, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to websocket %s : %w", wsUrl, err)
	}

	streamChannel := make(chan rxgo.Item)
	resultStream := rxgo.FromChannel(streamChannel, rxgo.WithPublishStrategy())

	go func() {
		defer close(streamChannel)
		defer c.Close()

		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				return
			}
			var messagePayload []interface{}
			json.NewDecoder(bytes.NewBuffer(message)).Decode(&messagePayload)
			event := toEvent(queryResult.Result.Header, messagePayload)
			streamChannel <- rxgo.Of(event)
		}
	}()
	return resultStream, nil
}
