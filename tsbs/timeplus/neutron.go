package timeplus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/timeplus-io/chameleon/tsbs/utils"
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

type NeutronServer struct {
	address string
	apikey  string
	client  *http.Client
}

func NewNeutronServer(address string, apikey string) *NeutronServer {
	return &NeutronServer{
		address: address,
		apikey:  apikey,
		client:  utils.NewDefaultHttpClient(),
	}
}

func (s *NeutronServer) CreateStream(streamDef StreamDef) error {
	url := fmt.Sprintf("%s/api/%s/streams", s.address, API_VERSION)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, streamDef, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", streamDef.Name, err)
	}
	return nil
}

func (s *NeutronServer) DeleteStream(streamName string) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s", s.address, API_VERSION, streamName)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodDelete, url, nil, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", streamName, err)
	}
	return nil
}

func (s *NeutronServer) ListStream() ([]StreamDef, error) {
	url := fmt.Sprintf("%s/api/%s/streams", s.address, API_VERSION)
	_, respBody, err := utils.HttpRequestWithAPIKey(http.MethodGet, url, nil, s.client, s.apikey)
	if err != nil {
		return nil, fmt.Errorf("failed to list stream : %w", err)
	}

	var payload []StreamDef
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&payload)

	return payload, nil
}

func (s *NeutronServer) InsertData(data IngestPayload) error {
	url := fmt.Sprintf("%s/api/%s/streams/%s/ingest", s.address, API_VERSION, data.Stream)
	_, _, err := utils.HttpRequestWithAPIKey(http.MethodPost, url, data.Data, s.client, s.apikey)
	if err != nil {
		return fmt.Errorf("failed to ingest data into stream %s: %w", data.Stream, err)
	}
	return nil
}
