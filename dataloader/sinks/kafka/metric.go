package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/timeplus-io/chameleon/dataloader/models"
	"github.com/timeplus-io/chameleon/dataloader/sinks"
)

func (writer *kafkaWriter) loadMetricData(source *models.Source, wg *sync.WaitGroup) {
	if source.Settings.Topic == "" {
		source.Settings.Topic = "device_utils"
	}

	if err := writer.newDeviceTopic(source); err != nil {
		return
	}

	writer.devLocations = sinks.GenerateLocations(source, true)

	for i := 0; i < int(source.Settings.Concurrency); i++ {
		wg.Add(1)
		go writer.doLoadMetricData(source, wg, i)
	}
}

func (writer *kafkaWriter) doLoadMetricData(source *models.Source, wg *sync.WaitGroup, i int) {
	defer wg.Done()

	var currentIteration int32
	/// batchSize := int(source.Settings.BatchSize)

	writer.logger.Info("target topic", zap.String("topic", source.Settings.Topic))

	start := time.Now().UnixNano()
	prev := start

	/// Generating records is very slow. We generate once and reuse it to avoid perf bottleneck on client side
	records := models.GenerateMetrics(source.Settings.BatchSize, writer.devLocations)
	payload, err := writer.marshal(records, source)
	if err != nil {
		return
	}

	for {
		/// records := models.GenerateMetrics(source.Settings.TotalEntities, writer.devLocations)

		atomic.AddUint64(&writer.ingested, (uint64)(len(records)))
		atomic.AddUint64(&writer.ingested_total, (uint64)(len(records)))

		err = writer.append(payload, source.Settings.Topic, source.Type)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		/*batch := records
		for n := 0; n < len(batch); n += batchSize {
			pos := n + batchSize
			if pos > len(batch) {
				pos = len(batch)
			}

			/// now := time.Now().UnixNano()
			writer.doMetricInsert(batch[n:pos], source.Settings.Topic, source.Type)
			/// atomic.AddUint64(&writer.duration, uint64(time.Now().UnixNano()-now))
			/// atomic.AddUint64(&writer.duration_total, uint64(time.Now().UnixNano()-now))

			currentIteration += 1
		}*/

		now := time.Now().UnixNano()
		if now-prev >= 2*1000*1000*1000 && i == 0 {
			current_duration_ms := uint64((now - prev) / 1000000)
			current_ingested := atomic.LoadUint64(&writer.ingested)

			ingested_total := atomic.LoadUint64(&writer.ingested_total)
			duration_total_ms := uint64((now - start) / 1000000)

			/// reset to 0
			atomic.StoreUint64(&writer.ingested, 0)

			writer.logger.Info("ingest metrics", zap.Uint64("ingested", current_ingested), zap.Uint64("duration_ms", current_duration_ms), zap.Uint64("eps", (current_ingested*1000)/current_duration_ms), zap.Uint64("ingested_total", ingested_total), zap.Uint64("duration_total_ms", duration_total_ms), zap.Uint64("overall_eps", (ingested_total*1000)/duration_total_ms))

			prev = now
		}

		if source.Settings.Iteration > 0 && currentIteration >= source.Settings.Iteration {
			break
		}

		if source.Settings.Interval > 0 {
			time.Sleep(time.Duration(source.Settings.Interval) * time.Millisecond)
		}
	}
}

func (writer *kafkaWriter) marshal(records []models.Metric, source *models.Source) ([]byte, error) {
	var data [][]byte
	/*if source.Settings.Format == "csv" {
		data = append(data, []byte("device,region,city,version,lat,lon,battery,humidity,temperature,hydraulic_pressure,atmospheric_pressure,timestamp"))
	}*/

	for _, record := range records {
		if source.Settings.Format == "json" {
			payload, err := json.Marshal(record)
			if err != nil {
				writer.logger.Error("Failed to marshal base.Data object", zap.Error(err))
				return nil, err
			}
			data = append(data, payload)
		} else if source.Settings.Format == "csv" {
			payload := fmt.Sprintf("%s,%s,%s,%s,%g,%g,%g,%d,%d,%g,%g,%d", record.Device, record.Region, record.City, record.Version, record.Lat, record.Lon, record.Battery, record.Humidity, record.Temperature, record.HydraulicPressure, record.AtmosphericPressure, record.Timestamp.UnixMilli())
			data = append(data, []byte(payload))
		}
	}

	return bytes.Join(data, []byte("\n")), nil
}

func (writer *kafkaWriter) appendMetrics(records []models.Metric, topic, typ string) error {
	var data [][]byte
	for _, record := range records {
		payload, err := json.Marshal(record)
		if err != nil {
			writer.logger.Error("Failed to marshal base.Data object", zap.Error(err))
			return err
		}

		data = append(data, payload)
	}

	batch_payload := bytes.Join(data, []byte("\n"))

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   nil,
		Value: sarama.StringEncoder(batch_payload),
	}

	return writer.write(msg)
}

func (writer *kafkaWriter) append(payload []byte, topic, typ string) error {
	now := time.Now().UnixMilli()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   nil,
		Value: sarama.StringEncoder(payload),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("_tp_time"),
				Value: []byte(strconv.FormatInt(now, 10)), /// FormatInt is slow, but anyway
			},
		},
	}

	return writer.write(msg)
}

func (writer *kafkaWriter) newDeviceTopic(source *models.Source) error {
	topic := source.Settings.Topic
	topics_metadata, err := writer.admin.DescribeTopics([]string{topic})
	if err != nil {
		writer.logger.Error("Failed to describe topic", zap.String("topic", topic), zap.Error(err))
		return err
	}

	topic_exists := false
	// Check describe error
	// https://pkg.go.dev/github.com/shopify/sarama#KError
	// ErrUnknownTopicOrPartition
	if topics_metadata[0].Err == sarama.ErrNoError {
		topic_exists = true
	}

	if topic_exists && !source.Settings.CleanBeforeLoad {
		// Topic exists and we don't need delete the topic before data load
		return nil
	}

	if topic_exists && source.Settings.CleanBeforeLoad {
		// Topic exists and we need clean it up before data load
		err = writer.admin.DeleteTopic(topic)
		if err != nil {
			writer.logger.Error("Failed to delete topic", zap.String("topic", topic), zap.Error(err))
			return err
		} else {
			writer.logger.Info("Successfully delete topic", zap.String("topic", topic))
		}
	}

	err = writer.admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     source.Settings.NumPartitions,
		ReplicationFactor: source.Settings.ReplicationFactor,
	}, false)

	if err != nil {
		writer.logger.Error("Failed to create topic", zap.String("topic", topic), zap.Error(err))
	} else {
		writer.logger.Info("Successfully create topic", zap.String("topic", topic))
	}

	return err
}
