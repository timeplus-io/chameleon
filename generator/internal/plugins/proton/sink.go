package proton

import (
	"fmt"

	"github.com/timeplus-io/chameleon/generator/internal/common"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/source"
	"github.com/timeplus-io/chameleon/generator/internal/utils"
)

const ProtonSinkType = "proton"

const DefaultTTL = "to_datetime(_tp_time) + INTERVAL 30 DAY"
const DefaultLogStoreRetentionBytes = 604800000
const DefaultLogStoreRetentionMS = 1342177280

type ProtonSink struct {
	client     *Client
	streamName string
}

func NewProtonSink(properties map[string]interface{}) (sink.Sink, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "port", 8123)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	username, err := utils.GetWithDefault(properties, "username", "default")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	password, err := utils.GetWithDefault(properties, "password", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &ProtonSink{
		client: NewClient(host, port, username, password),
	}, nil
}

func convertType(sourceType string) string {
	switch sourceType {
	case string(source.FIELDTYPE_TIMESTAMP):
		return "datetime64(3)"
	case string(source.FIELDTYPE_TIMESTAMP_INT):
		return "int64"
	case string(source.FIELDTYPE_STRING):
		return "string"
	case string(source.FIELDTYPE_INT):
		return "int64"
	case string(source.FIELDTYPE_FLOAT):
		return "float64"
	case string(source.FIELDTYPE_BOOL):
		return "bool"
	case string(source.FIELDTYPE_MAP):
	case string(source.FIELDTYPE_ARRAY):
	case string(source.FIELDTYPE_GENERATE):
	case string(source.FIELDTYPE_REGEX):
		return "string"
	}
	return "string"
}

func (s *ProtonSink) Init(name string, fields []common.Field) error {
	s.streamName = name

	streamDef := StreamDef{
		Name:          name,
		Columns:       make([]ColumnDef, len(fields)),
		TTLExpression: DefaultTTL,
	}

	for index, field := range fields {
		convertedType := convertType(field.Type)
		log.Logger().Debugf("convert type %s to %s", field.Type, convertedType)

		streamDef.Columns[index] = ColumnDef{
			Name: field.Name,
			Type: convertedType,
		}
	}

	StreamStorageConfig := StreamStorageConfig{
		RetentionBytes: DefaultLogStoreRetentionBytes,
		RetentionMS:    DefaultLogStoreRetentionMS,
	}

	s.client.DeleteStream(streamDef.Name)
	return s.client.CreateStream(streamDef, StreamStorageConfig)

}

func (s *ProtonSink) Write(headers []string, rows [][]interface{}, index int) error {
	log.Logger().Debugf("Write one event to stream %s %v:%v", s.streamName, headers, rows)
	ingestData := IngestData{
		Columns: headers,
		Data:    rows,
	}

	if _, err := s.client.InsertData(ingestData, s.streamName); err != nil {
		return err
	}
	return nil
}

func (s *ProtonSink) GetStats() *sink.Stats {
	return &sink.Stats{
		SuccessWrite: 0,
		FailedWrite:  0,
	}
}
