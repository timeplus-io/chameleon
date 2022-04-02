package splunk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const SPLUNK_SINK_TYPE = "splunk"

type SplunkSink struct {
	client     *http.Client
	hecAddress string
	hecToken   string
	source     string
	sourcetype string
	index      string
}

type Event struct {
	time   int64        `json:"time"`
	host   string       `json:"host"`
	source string       `json:"source"`
	event  common.Event `json:"event"`
}

func NewSplunkSink(properties map[string]interface{}) (sink.Sink, error) {
	hecAddress, err := utils.GetWithDefault(properties, "hec_address", "https://localhost:8088/services/collector/event")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	hecToken, err := utils.GetWithDefault(properties, "hec_token", "abcd1234")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	source, err := utils.GetWithDefault(properties, "source", "my_source")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	sourcetype, err := utils.GetWithDefault(properties, "sourcetype", "my_source_type")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	index, err := utils.GetWithDefault(properties, "index", "main")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &SplunkSink{
		client:     utils.NewDefaultHttpClient(),
		hecAddress: hecAddress,
		hecToken:   hecToken,
		source:     source,
		sourcetype: sourcetype,
		index:      index,
	}, nil
}

func (s *SplunkSink) Init(name string, fields []common.Field) error {
	return nil
}

func (s *SplunkSink) Write(headers []string, rows [][]interface{}) error {
	events := s.ToSplunkEvents(common.ToEvents(headers, rows))
	log.Logger().Debugf("Write one event to splunk %v", events)

	hecUrl := "http://localhost:8088/services/collector/event"
	_, respBody, err := utils.HttpRequestWithAuth(http.MethodPost, hecUrl, events, s.client, "Splunk abcd1234")
	if err != nil {
		return fmt.Errorf("failed to insert data : %w", err)
	}

	var queryResult map[string]interface{}
	json.NewDecoder(bytes.NewBuffer(respBody)).Decode(&queryResult)
	log.Logger().Debugf("the insert result is %v", queryResult)
	return nil
}

func (s *SplunkSink) ToSplunkEvents(events []common.Event) []map[string]interface{} {
	result := make([]map[string]interface{}, len(events))
	for index, event := range events {
		result[index] = map[string]interface{}{
			"source": s.source,
			"host":   "localhost",
			"time":   time.Now().Unix(),
			"event":  event,
		}
	}
	return result
}
