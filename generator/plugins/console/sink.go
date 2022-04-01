package console

import (
	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
)

const CONSOLE_SINK_TYPE = "console"

type Console struct{}

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        CONSOLE_SINK_TYPE,
		Constructor: NewConsoleSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", CONSOLE_SINK_TYPE)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}

func NewConsoleSink(properties map[string]interface{}) (sink.Sink, error) {
	return &Console{}, nil
}

func (s *Console) Write(headers []string, rows [][]interface{}) error {
	log.Logger().Infof("Write one event to console %v:%v", headers, rows)
	return nil
}

func (s *Console) Init(name string, fields []common.Field) error {
	return nil
}
