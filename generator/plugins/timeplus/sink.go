package timeplus

import (
	"fmt"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const TimeplusSinkType = "timeplus"

type TimeplusSink struct {
	server     *TimeplusServer
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
	return &TimeplusSink{
		server: NewTimeplusServer(address, apikey),
	}, nil
}

func convertType(sourceType string) string {
	switch sourceType {
	case string(source.FIELDTYPE_TIMESTAMP):
		return "datetime64(3)"
	case string(source.FIELDTYPE_TIMESTAMP_INT):
		return "int"
	case string(source.FIELDTYPE_STRING):
		return "string"
	case string(source.FIELDTYPE_INT):
		return "int"
	case string(source.FIELDTYPE_FLOAT):
		return "float"
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

	streamDef := StreamDef{
		Name:                   name,
		Columns:                make([]ColumnDef, len(fields)),
		TTLExpression:          DefaultTTL,
		LogStoreRetentionBytes: DefaultLogStoreRetentionBytes,
		LogStoreRetentionMS:    DefaultLogStoreRetentionMS,
	}

	for index, field := range fields {
		convertedType := convertType(field.Type)
		log.Logger().Debugf("convert type %s to %s", field.Type, convertedType)

		streamDef.Columns[index] = ColumnDef{
			Name: field.Name,
			Type: convertedType,
		}
	}

	s.server.DeleteStream(streamDef.Name)
	return s.server.CreateStream(streamDef)
}

func (s *TimeplusSink) Write(headers []string, rows [][]interface{}, index int) error {
	log.Logger().Debugf("Write one event to stream %s %v:%v", s.streamName, headers, rows)
	ingestData := IngestPayload{
		Stream: s.streamName,
		Data: IngestData{
			Columns: headers,
			Data:    rows,
		},
	}

	//log.Logger().Infof("Write one event to stream %v", ingestData)

	return s.server.InsertData(ingestData)
}
