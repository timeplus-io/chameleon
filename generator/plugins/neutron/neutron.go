package neutron

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/reactivex/rxgo/v2"
	"github.com/spf13/viper"
	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
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

type DataRow []interface{}

type IngestData struct {
	Columns []string  `json:"columns"`
	Data    []DataRow `json:"data"`
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
		client: NewDefaultHttpClient(),
	}
}

func NewDefaultHttpClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = viper.GetInt("http-max-idle-connection")
	t.MaxConnsPerHost = viper.GetInt("http-max-connection-per-host")
	t.MaxIdleConnsPerHost = viper.GetInt("http-max-idle-connection-per-host")

	return &http.Client{
		Timeout:   viper.GetDuration("http-timeout") * time.Second,
		Transport: t,
	}
}

// request will propragate error if the response code is not 2XX
func (s *NeutronServer) request(method string, url string, payload interface{}) (int, []byte, error) {
	var body io.Reader
	if payload == nil {
		body = nil
		log.Logger().Debugf("send empty request to url %s", url)
	} else {
		jsonPostValue, _ := json.Marshal(payload)
		body = bytes.NewBuffer(jsonPostValue)
		log.Logger().Debugf("send request %s to url %s", string(jsonPostValue), url)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return 0, nil, err
	}
	//req.SetBasicAuth(s.user, s.password)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	res, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}

	defer res.Body.Close()
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, nil, err
	}

	if res.StatusCode > 299 || res.StatusCode < 200 {
		return res.StatusCode, resBody, fmt.Errorf("request failed with status code %d, response body %s", res.StatusCode, resBody)
	}

	return res.StatusCode, resBody, nil

}

func (s *NeutronServer) CreateStream(streamDef StreamDef) error {
	url := fmt.Sprintf("%s/api/%s/streams", s.addres, API_VERSION)
	_, _, err := s.request(http.MethodPost, url, streamDef)
	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", streamDef.Name, err)
	}
	return nil
}

func (s *NeutronServer) DeleteStream(streamName string) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s", s.addres, API_VERSION, streamName)
	_, _, err := s.request(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", streamName, err)
	}
	return nil
}

func (s *NeutronServer) ListStream() ([]StreamDef, error) {
	url := fmt.Sprintf("%s/api/%s/streams", s.addres, API_VERSION)
	_, respBody, err := s.request(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list stream : %w", err)
	}

	var payload []StreamDef
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)

	return payload, nil
}

func (s *NeutronServer) InsertData(data IngestPayload) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s/ingest", s.addres, API_VERSION, data.Stream)
	_, _, err := s.request(http.MethodPost, url, data.Data)
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
	_, respBody, err := s.request(http.MethodPost, createQueryUrl, query)
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
			log.Logger().Debugf("read message from websocket : %v", event)
			streamChannel <- rxgo.Of(event)
		}
	}()
	return resultStream, nil
}
