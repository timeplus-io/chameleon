package neutron

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/viper"
	"github.com/timeplus-io/chameleon/generator/log"
)

const TIME_FORMAT = "2006-01-02 15:04:05.000"
const API_VERSION = "v1beta1"

type ColumnDef struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Default  string `json:"default,omitempty"`
	Nullable bool   `json:"nullable,omitempty"`
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

func (s *NeutronServer) QueryStream() {

}
