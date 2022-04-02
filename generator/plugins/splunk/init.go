package splunk

import (
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
)

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        SPLUNK_SINK_TYPE,
		Constructor: NewSplunkSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", SPLUNK_SINK_TYPE)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}
