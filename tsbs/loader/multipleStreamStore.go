package loader

import (
	"sync"

	"github.com/timeplus-io/chameleon/tsbs/common"
	"github.com/timeplus-io/chameleon/tsbs/log"
	"github.com/timeplus-io/chameleon/tsbs/utils"

	timeplus "github.com/timeplus-io/go-client/client"
)

type MultipleStreamStoreLoader struct {
	client         *timeplus.TimeplusClient
	metrics        []common.Metric
	realtimeIngest bool
}

func NewMultipleStreamStoreLoader(client *timeplus.TimeplusClient, metrics []common.Metric, realtimeIngest bool) *MultipleStreamStoreLoader {
	return &MultipleStreamStoreLoader{
		client:         client,
		metrics:        metrics,
		realtimeIngest: realtimeIngest,
	}
}

func (l *MultipleStreamStoreLoader) DeleteStreams() {
	for _, metric := range l.metrics {
		if err := l.client.DeleteStream(metric.Name); err != nil {
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
		Name:                   metric.Name,
		EventTimeColumn:        "to_datetime64(timestamp,9)",
		TTLExpression:          DefaultTTL,
		LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
		LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
	}

	cols := []timeplus.ColumnDef{}
	timeCol := timeplus.ColumnDef{
		Name: "timestamp",
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
	return l.client.CreateStream(streamDef)
}

func (l *MultipleStreamStoreLoader) Ingest(payloads []common.Payload) {
	var wg sync.WaitGroup
	splittedPayloads := common.SplitPayloads(payloads)
	for key := range splittedPayloads {
		wg.Add(1)
		go l.ingestProcess(splittedPayloads[key], &wg)
	}
	wg.Wait()
}

func (l *MultipleStreamStoreLoader) ingestProcess(payloads []common.Payload, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		for _, payload := range payloads {
			metricName := payload.Name
			metric := common.FindMetricByName(l.metrics, metricName)
			measureNames := metric.GetMeasureNames()
			tagNames := payload.Tags

			headers := append([]string{"timestamp"}, tagNames...)
			headers = append(headers, measureNames...)

			data := make([][]interface{}, 1)
			data[0] = payload.Data

			if l.realtimeIngest {
				data[0][0] = utils.GetTimestamp()
			}

			load := timeplus.IngestPayload{
				Stream: metricName,
				Data: timeplus.IngestData{
					Columns: headers,
					Data:    data,
				},
			}

			if err := l.client.InsertData(load); err != nil {
				log.Logger().WithError(err).Errorf("failed to ingest")
			}
		}

		if !l.realtimeIngest {
			break
		}
	}

}
