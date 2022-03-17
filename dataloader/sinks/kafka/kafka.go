package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/timeplus-io/chameleon/dataloader/models"
	"github.com/timeplus-io/chameleon/dataloader/sinks"
)

type kafkaWriter struct {
	config *models.Config

	devLocations            map[string][]models.LatLon
	unstructureDevLocations map[string][]models.LatLon
	personLocations         map[string][]models.LatLon

	syncProducer sarama.SyncProducer
	// admin client
	admin sarama.ClusterAdmin

	ingested_total uint64
	duration_total uint64

	ingested uint64
	duration uint64

	logger *zap.Logger
}

func init() {
	sinks.RegisterSink("kafka", NewkafkaWriter)
}

// NewKafaWriter
// FIXME support more config options
// `config` contains
//{
//	"kafka_brokers": "<ip:port;ip:port;...>",
//	"kafka_topic": "<my_topic>",
//  "require_acks": "<true>|<false>",
//  "flush_memory": "<true>|<false>",
//  "write_mode": "<sync>|<async>",
//}
func NewkafkaWriter(config *models.Config, logger *zap.Logger) (sinks.Sink, error) {
	kconfig := sarama.NewConfig()
	kconfig.Producer.RequiredAcks = sarama.WaitForLocal

	if len(config.Sources) > 0 {
		/// Hack
		/// Use the first source settings
		kconfig.Producer.Flush.Frequency = time.Duration(config.Sources[0].Settings.FlushFrequencyMs) * time.Millisecond
		kconfig.Producer.Flush.Messages = config.Sources[0].Settings.FlushMessagesThreshold
		kconfig.Producer.Flush.Bytes = config.Sources[0].Settings.FlushBytesThreshold
		kconfig.Producer.Flush.MaxMessages = config.Sources[0].Settings.MaxBufferedMessages
		kconfig.Producer.MaxMessageBytes = config.Sources[0].Settings.MaxMessageBytes
	}

	logger.Info("Kafka producer flush settings", zap.Int("frequency", int(kconfig.Producer.Flush.Frequency)), zap.Int("messages", kconfig.Producer.Flush.Messages), zap.Int("bytes", kconfig.Producer.Flush.Bytes), zap.Int("max_messages", kconfig.Producer.Flush.MaxMessages), zap.Int("max_message_bytes", kconfig.Producer.MaxMessageBytes))

	kconfig.Producer.Return.Successes = true

	syncProducer, err := sarama.NewSyncProducer(config.Sink.Addresses, kconfig)
	if err != nil {
		logger.Error("Failed to create kafka sync producer", zap.Error(err))
		return nil, err
	}

	adminConfig := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(config.Sink.Addresses, adminConfig)
	if err != nil {
		logger.Error("Failed to create kafka admin client", zap.Error(err))
		return nil, err
	}

	return &kafkaWriter{
		config:       config,
		logger:       logger,
		syncProducer: syncProducer,
		admin:        admin,
	}, nil
}

func (writer *kafkaWriter) LoadData() {
	var wg sync.WaitGroup
	for i := range writer.config.Sources {
		if writer.config.Sources[i].Enabled {
			writer.loadDataFor(&writer.config.Sources[i], &wg)
		}
	}
	wg.Wait()
}

func (writer *kafkaWriter) loadDataFor(source *models.Source, wg *sync.WaitGroup) {
	if source.Type == models.METRIC {
		writer.loadMetricData(source, wg)
	} else {
		writer.logger.Error("unsupported data type", zap.String("type", source.Type))
	}

	/*else if source.Type == models.LOG {
		writer.loadLogData(source, wg)
	} else if source.Type == models.CRIME_CASE {
		writer.loadCrimeData(source, wg)
	} else if source.Type == models.PERSON {
		writer.loadPersonData(source, wg)
	} else if source.Type == models.UNSTRUCTURE_METRIC {
		writer.loadUnstructureData(source, wg)
	} */
}

func (writer *kafkaWriter) write(msg *sarama.ProducerMessage) error {
	_, _, err := writer.syncProducer.SendMessage(msg)
	// FIXME retry other brokers when failed ?
	if err != nil {
		writer.logger.Error(
			"Failed to write data to kafka",
			zap.String("topic", msg.Topic), zap.Int32("partition", msg.Partition), zap.Error(err))
	}
	return err
}

func (writer *kafkaWriter) Stop() {
	writer.syncProducer.Close()
	writer.admin.Close()
	writer.logger.Info("kafkaWriter stopped...")
}
