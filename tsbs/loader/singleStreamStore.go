package loader

import (
	"strconv"

	"github.com/timeplus-io/chameleon/tsbs/common"
	"github.com/timeplus-io/chameleon/tsbs/log"
	"github.com/timeplus-io/chameleon/tsbs/timeplus"
)

const SingleStoreStreamName = "metrics"

type SingleStreamStoreLoader struct {
	server  *timeplus.NeutronServer
	metrics []common.Metric
}

func NewSingleStreamStoreLoader(server *timeplus.NeutronServer, metrics []common.Metric) *SingleStreamStoreLoader {
	return &SingleStreamStoreLoader{
		server:  server,
		metrics: metrics,
	}
}

func (l *SingleStreamStoreLoader) DeleteStreams() {
	l.server.DeleteStream(SingleStoreStreamName)
}

func (l *SingleStreamStoreLoader) CreateStreams() error {
	streamDef := timeplus.StreamDef{
		Name: SingleStoreStreamName,
		Columns: []timeplus.ColumnDef{
			{
				Name: "metric",
				Type: "string",
			},
			{
				Name: "timestamp",
				Type: "float64",
			},
			{
				Name: "tags",
				Type: "map(string, string)",
			},
			{
				Name: "value",
				Type: "float64",
			},
		},
	}

	return l.server.CreateStream(streamDef)
}

func (l *SingleStreamStoreLoader) Ingest(payloads []common.Payload) {
	for _, payload := range payloads {
		l.ingest(payload)
	}
}

func (l *SingleStreamStoreLoader) ingest(payload common.Payload) {
	log.Logger().Debugf("ingest one observe %v", payload)
	headers := []string{"metric", "timestamp", "tags", "value"}
	data := l.buildPayloadData(payload)

	load := timeplus.IngestPayload{
		Stream: SingleStoreStreamName,
		Data: timeplus.IngestData{
			Columns: headers,
			Data:    data,
		},
	}

	if err := l.server.InsertData(load); err != nil {
		log.Logger().WithError(err).Fatalf("failed to ingest")
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
		if s, err := strconv.ParseFloat(payload.Timestamp, 64); err == nil {
			row = append(row, s)
		} else {
			row = append(row, 0)
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
