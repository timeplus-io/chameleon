package elastic_search

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/utils"
	"strings"
	"time"
)

const ELASTIC_SEARCH_SINK_TYPE = "elastic_search"

type ElasticSearchSink struct {
	bulkIndexer esutil.BulkIndexer
	addresses   []string
	index       string
}

type Event struct {
	time  int64        `json:"time"`
	host  string       `json:"host"`
	index string       `json:"index"`
	event common.Event `json:"event"`
}

func NewElasticSearchSink(properties map[string]interface{}) (sink.Sink, error) {
	addresses, err := utils.GetStringListWithDefault(properties, "addresses", []string{"http://localhost:9200"})
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	index, err := utils.GetWithDefault(properties, "index", "main")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	cfg := elasticsearch.Config{
		Addresses: addresses,
	}

	client, err := elasticsearch.NewClient(cfg)

	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         index,                 // The default index name
		Client:        client,                // The Elasticsearch client
		NumWorkers:    10,                    // The number of worker goroutines
		FlushBytes:    int(1024),             // The flush threshold in bytes
		FlushInterval: 10 * time.Millisecond, // The periodic flush interval
	})

	return &ElasticSearchSink{
		bulkIndexer: bulkIndexer,
		addresses:   addresses,
		index:       index,
	}, nil
}

func (s *ElasticSearchSink) Init(name string, fields []common.Field) error {
	return nil
}

func (s *ElasticSearchSink) Write(headers []string, rows [][]interface{}, index int) error {
	bulkIndexer := s.bulkIndexer
	events := s.ToElasticSearchEvents(common.ToEvents(headers, rows))
	log.Logger().Debugf("Write one event to elastic search %v", events)
	for _, event := range events {
		eb, err := json.Marshal(event)
		if err != nil {
			fmt.Errorf("failed to marshal event : %w", err)
		}

		bulkIndexer.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   strings.NewReader(string(eb)),
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						fmt.Errorf("failed to insert data : %w", err)
					} else {
						fmt.Errorf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
	}

	return nil
}

func (s *ElasticSearchSink) ToElasticSearchEvents(events []common.Event) []map[string]interface{} {
	result := make([]map[string]interface{}, len(events))
	for index, event := range events {
		result[index] = map[string]interface{}{
			"index": s.index,
			"host":  "localhost",
			"time":  time.Now().Unix(),
			"event": event,
		}
	}
	return result
}
