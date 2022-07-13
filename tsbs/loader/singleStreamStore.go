package loader

import (
	"sync"

	"github.com/timeplus-io/chameleon/tsbs/common"
	"github.com/timeplus-io/chameleon/tsbs/log"
	"github.com/timeplus-io/chameleon/tsbs/utils"

	timeplus "github.com/timeplus-io/go-client/client"
)

type SingleStreamStoreLoader struct {
	client         *timeplus.TimeplusClient
	metrics        []common.Metric
	name           string
	realtimeIngest bool
}

func NewSingleStreamStoreLoader(client *timeplus.TimeplusClient, metrics []common.Metric, name string, realtimeIngest bool) *SingleStreamStoreLoader {
	return &SingleStreamStoreLoader{
		client:         client,
		metrics:        metrics,
		name:           name,
		realtimeIngest: realtimeIngest,
	}
}

func (l *SingleStreamStoreLoader) DeleteStreams() {
	l.client.DeleteStream(l.name)
}

func (l *SingleStreamStoreLoader) CreateStreams() error {
	log.Logger().Infof("Create stream with name %s", l.name)
	streamDef := timeplus.StreamDef{
		Name: l.name,
		Columns: []timeplus.ColumnDef{
			{
				Name: "metric",
				Type: "string",
			},
			{
				Name: "timestamp",
				Type: "string",
			},
			{
				Name: "tags",
				Type: "json",
			},
			{
				Name: "value",
				Type: "float64",
			},
		},
		EventTimeColumn:        "to_datetime64(timestamp,9)",
		TTLExpression:          DefaultTTL,
		LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
		LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
	}

	return l.client.CreateStream(streamDef)
}

func (l *SingleStreamStoreLoader) Ingest(payloads []common.Payload) {
	var wg sync.WaitGroup
	splittedPayloads := common.SplitPayloads(payloads)
	for key := range splittedPayloads {
		wg.Add(1)
		go l.ingestProcess(splittedPayloads[key], &wg)
	}
	wg.Wait()
}

func (l *SingleStreamStoreLoader) ingestProcess(payloads []common.Payload, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		for _, payload := range payloads {
			l.ingest(payload)
		}
		if !l.realtimeIngest {
			break
		}
	}
}

func (l *SingleStreamStoreLoader) ingest(payload common.Payload) {
	log.Logger().Debugf("ingest one observe %v", payload)
	headers := []string{"metric", "timestamp", "tags", "value"}
	data := l.buildPayloadData(payload)

	load := timeplus.IngestPayload{
		Stream: l.name,
		Data: timeplus.IngestData{
			Columns: headers,
			Data:    data,
		},
	}

	if err := l.client.InsertData(load); err != nil {
		log.Logger().WithError(err).Warnf("failed to ingest")
	}
}

func (l *SingleStreamStoreLoader) buildPayloadData(payload common.Payload) [][]interface{} {
	tagSize := len(payload.Tags)
	tagValues := payload.Data[1 : 1+tagSize]
	values := payload.Data[1+tagSize:]
	metric := common.FindMetricByName(l.metrics, payload.Name)
	result := make([][]interface{}, len(values))

	for index, value := range values {
		row := []interface{}{}
		// name cell
		row = append(row, payload.Name)

		// timestamp cell
		if l.realtimeIngest {
			row = append(row, utils.GetTimestamp())
		} else {
			row = append(row, payload.Timestamp)
		}

		// tags cell
		tags := map[string]interface{}{}
		tags["category"] = metric.Values[index].Name
		for tagIndex, tagValue := range tagValues {
			tags[payload.Tags[tagIndex]] = tagValue.(string)
		}
		row = append(row, tags)

		// value cell
		row = append(row, value)

		result[index] = row
	}

	return result
}
