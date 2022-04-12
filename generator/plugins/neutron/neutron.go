package neutron

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/reactivex/rxgo/v2"
	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const TIME_FORMAT = "2006-01-02 15:04:05.000"
const API_VERSION = "v1beta1"

type ColumnDef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type StreamDef struct {
	Name              string      `json:"name"`
	Columns           []ColumnDef `json:"columns"`
	EventTimeColumn   string      `json:"event_time_column,omitempty"`
	Shards            int         `json:"shards,omitempty"`
	ReplicationFactor int         `json:"replication_factor,omitempty"`
	TTLExpression     string      `json:"ttl_expression,omitempty"`
}

type IngestData struct {
	Columns []string        `json:"columns"`
	Data    [][]interface{} `json:"data"`
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

type NeutronServer struct {
	addres string
	client *http.Client
}

func NewNeutronServer(address string) *NeutronServer {
	return &NeutronServer{
		addres: address,
		client: utils.NewDefaultHttpClient(),
	}
}

func (s *NeutronServer) SyncQuery(sql string) (*QueryResult, error) {
	url := fmt.Sprintf("%s/api/%s/sql", s.addres, API_VERSION)

	req := SQLRequest{
		SQL:     sql,
		Timeout: 1000,
	}

	_, respBody, err := utils.HttpRequest(http.MethodPost, url, req, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to run SQL %s: %w", req.SQL, err)
	}

	var payload QueryResult
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)
	return &payload, nil
}

func (s *NeutronServer) CreateStream(streamDef StreamDef) error {
	url := fmt.Sprintf("%s/api/%s/streams", s.addres, API_VERSION)
	_, _, err := utils.HttpRequest(http.MethodPost, url, streamDef, s.client)
	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", streamDef.Name, err)
	}
	return nil
}

func (s *NeutronServer) DeleteStream(streamName string) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s", s.addres, API_VERSION, streamName)
	_, _, err := utils.HttpRequest(http.MethodDelete, url, nil, s.client)
	if err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", streamName, err)
	}
	return nil
}

func (s *NeutronServer) ListStream() ([]StreamDef, error) {
	url := fmt.Sprintf("%s/api/%s/streams", s.addres, API_VERSION)
	_, respBody, err := utils.HttpRequest(http.MethodGet, url, nil, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list stream : %w", err)
	}

	var payload []StreamDef
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)

	return payload, nil
}

func (s *NeutronServer) InsertData(data IngestPayload) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s/ingest", s.addres, API_VERSION, data.Stream)
	_, _, err := utils.HttpRequest(http.MethodPost, url, data.Data, s.client)
	if err != nil {
		return fmt.Errorf("failed to ingest data into stream %s: %w", data.Stream, err)
	}
	return nil
}

func toEvent(headers []ColumnDef, data []interface{}) common.Event {
	event := make(common.Event)
	for index, header := range headers {
		event[header.Name] = data[index]
	}
	return event
}

func (s *NeutronServer) QueryStream(sql string) (rxgo.Observable, error) {
	query := Query{
		SQL:         sql,
		Name:        "",
		Description: "",
		Tags:        []string{},
	}

	createQueryUrl := fmt.Sprintf("%s/api/%s/queries", s.addres, API_VERSION)
	_, respBody, err := utils.HttpRequest(http.MethodPost, createQueryUrl, query, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to create query : %w", err)
	}

	var queryResult QueryInfo
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&queryResult)
	log.Logger().Debugf("the created query is %v", queryResult)

	// getQueryUrl := fmt.Sprintf("%s/api/%s/queries/%s", s.addres, API_VERSION, queryResult.ID)
	// _, respBody, err = s.request(http.MethodGet, getQueryUrl, nil)
	// if err != nil {
	// 	return fmt.Errorf("failed to get query : %w", err)
	// }

	// json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&queryResult)
	// log.Logger().Infof("the get query is %v", queryResult)

	wsUrl := fmt.Sprintf("%s/ws/queries/%s", s.addres, queryResult.ID)
	wsUrl = strings.Replace(wsUrl, "http", "ws", 1)

	log.Logger().Debugf("the ws url is %v", wsUrl)

	c, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to websocket %s : %w", wsUrl, err)
	}

	streamChannel := make(chan rxgo.Item)
	resultStream := rxgo.FromChannel(streamChannel)

	go func() {
		defer close(streamChannel)
		defer c.Close()

		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Logger().Warnf("failed to read message from websocket", err)
				return
			}
			var messagePayload []interface{}
			json.NewDecoder(bytes.NewBuffer(message)).Decode(&messagePayload)
			event := toEvent(queryResult.Result.Header, messagePayload)
			log.Logger().Infof("read message from websocket : %v", event)
			streamChannel <- rxgo.Of(event)
		}
	}()
	return resultStream, nil
}
