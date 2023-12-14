package timeplus

import (
	"fmt"

	"github.com/timeplus-io/chameleon/generator/internal/common"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/source"
	"github.com/timeplus-io/chameleon/generator/internal/utils"

	"github.com/timeplus-io/go-client/timeplus"
	timeplusUtils "github.com/timeplus-io/go-client/utils"
)

const TimeplusSinkType = "timeplus"

const DefaultTTL = "to_datetime(_tp_time) + INTERVAL 30 DAY"
const DefaultLogStoreRetentionBytes = 604800000
const DefaultLogStoreRetentionMS = 1342177280

type TimeplusSink struct {
	server     *timeplus.TimeplusClient
	streamName string
}

func NewTimeplusSink(properties map[string]interface{}) (sink.Sink, error) {
	address, err := utils.GetWithDefault(properties, "address", "http://localhost:8000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	apikey, err := utils.GetWithDefault(properties, "apikey", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	tenant, err := utils.GetWithDefault(properties, "tenant", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	insecureSkipVerify, err := utils.GetBoolWithDefault(properties, "insecureSkipVerify", true)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	maxIdleConns, err := utils.GetIntWithDefault(properties, "maxIdleConns", 100)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	maxConnsPerHost, err := utils.GetIntWithDefault(properties, "maxIdleConns", 100)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	maxIdleConnsPerHost, err := utils.GetIntWithDefault(properties, "maxIdleConnsPerHost", 100)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeout, err := utils.GetIntWithDefault(properties, "http_timeout", 10)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	config := timeplusUtils.NewHTTPClientConfig(insecureSkipVerify, maxIdleConns, maxConnsPerHost, maxIdleConnsPerHost, timeout)
	return &TimeplusSink{
		server: timeplus.NewCientWithHttpConfig(address, tenant, apikey, config),
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

func (s *TimeplusSink) Init(name string, fields []common.Field) error {
	s.streamName = name

	streamDef := timeplus.StreamDef{
		Name:                   name,
		Columns:                make([]timeplus.ColumnDef, len(fields)),
		TTLExpression:          DefaultTTL,
		LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
		LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
	}

	for index, field := range fields {
		convertedType := convertType(field.Type)
		log.Logger().Debugf("convert type %s to %s", field.Type, convertedType)

		streamDef.Columns[index] = timeplus.ColumnDef{
			Name: field.Name,
			Type: convertedType,
		}
	}

	s.server.DeleteStream(streamDef.Name)
	return s.server.CreateStream(streamDef)
}

func (s *TimeplusSink) Write(headers []string, rows [][]interface{}, index int) error {
	log.Logger().Debugf("Write one event to stream %s %v:%v", s.streamName, headers, rows)
	ingestData := &timeplus.IngestPayload{
		Stream: s.streamName,
		Data: timeplus.IngestData{
			Columns: headers,
			Data:    rows,
		},
	}

	return s.server.InsertData(ingestData)
}
