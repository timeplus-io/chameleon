package loader

import (
	"github.com/timeplus-io/chameleon/tsbs/common"
	"github.com/timeplus-io/chameleon/tsbs/log"
	"github.com/timeplus-io/chameleon/tsbs/timeplus"
)

type MultipleStreamStoreLoader struct {
	server  *timeplus.NeutronServer
	metrics []common.Metric
}

func NewMultipleStreamStoreLoader(server *timeplus.NeutronServer, metrics []common.Metric) *MultipleStreamStoreLoader {
	return &MultipleStreamStoreLoader{
		server:  server,
		metrics: metrics,
	}
}

func (l *MultipleStreamStoreLoader) DeleteStreams() {
	for _, metric := range l.metrics {
		if err := l.server.DeleteStream(metric.Name); err != nil {
			log.Logger().WithError(err).Warnf("failed to delete stream %s", metric.Name)
		} else {
			log.Logger().Infof("stream %s deleted", metric.Name)
		}
	}
}

func (l *MultipleStreamStoreLoader) CreateStreams() error {
	for _, metric := range l.metrics {
		if err := l.createStream(metric); err != nil {
			return err
		} else {
			log.Logger().Infof("stream %s created", metric.Name)
		}
	}
	return nil
}

func (l *MultipleStreamStoreLoader) createStream(metric common.Metric) error {
	streamDef := timeplus.StreamDef{
		Name: metric.Name,
	}

	cols := []timeplus.ColumnDef{}
	timeCol := timeplus.ColumnDef{
		Name: "ts",
		Type: "string",
	}
	cols = append(cols, timeCol)

	for _, tag := range metric.Tags {
		col := timeplus.ColumnDef{
			Name: tag.Name,
			Type: "string", // use string type for all tag for now
		}
		cols = append(cols, col)
	}

	for _, measure := range metric.Values {
		col := timeplus.ColumnDef{
			Name: measure.Name,
			Type: measure.Type,
		}
		cols = append(cols, col)
	}

	streamDef.Columns = cols
	return l.server.CreateStream(streamDef)
}

func (l *MultipleStreamStoreLoader) Ingest(payloads []common.Payload) {
	for _, payload := range payloads {
		metricName := payload.Name
		metric := common.FindMetricByName(l.metrics, metricName)
		measureNames := metric.GetMeasureNames()
		tagNames := payload.Tags

		headers := append([]string{"ts"}, tagNames...)
		headers = append(headers, measureNames...)

		data := make([][]interface{}, 1)
		data[0] = payload.Data

		load := timeplus.IngestPayload{
			Stream: metricName,
			Data: timeplus.IngestData{
				Columns: headers,
				Data:    data,
			},
		}

		if err := l.server.InsertData(load); err != nil {
			log.Logger().WithError(err).Fatalf("failed to ingest")
		}
	}
}
