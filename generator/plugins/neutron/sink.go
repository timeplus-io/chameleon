package neutron

import (
	"fmt"

	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const NEUTRON_SINK_TYPE = "neutron"

type NeutronSink struct {
	server     *NeutronServer
	streamName string
}

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        NEUTRON_SINK_TYPE,
		Constructor: NewNeutronSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", NEUTRON_SINK_TYPE)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}

func NewNeutronSink(properties map[string]interface{}) (sink.Sink, error) {
	address, err := utils.GetWithDefault(properties, "address", "http://localhost:8000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}
	return &NeutronSink{
		server: NewNeutronServer(address),
	}, nil
}

func convertType(sourceType string) string {
	switch sourceType {
	case string(source.FIELDTYPE_TIMESTAMP):
		return "datetime64(3)"
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

func (s *NeutronSink) Init(name string, fields []common.Field) error {
	s.streamName = name

	streamDef := StreamDef{
		Name:    name,
		Columns: make([]ColumnDef, len(fields)),
	}

	for index, field := range fields {
		convertedType := convertType(field.Type)
		log.Logger().Debugf("convert type %s to %s", field.Type, convertedType)

		streamDef.Columns[index] = ColumnDef{
			Name: field.Name,
			Type: convertedType,
		}
	}
	return s.server.CreateStream(streamDef)
}

func (s *NeutronSink) Write(headers []string, rows [][]interface{}) error {
	log.Logger().Debugf("Write one event to stream %s %v:%v", s.streamName, headers, rows)
	ingestData := IngestPayload{
		Stream: s.streamName,
		Data: IngestData{
			Columns: headers,
			Data:    rows,
		},
	}

	return s.server.InsertData(ingestData)
}
