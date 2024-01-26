package aerospike

import (
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
)

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        AEROSPIKE_SINK_TYPE,
		Constructor: NewAeroSpikeSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", AEROSPIKE_SINK_TYPE)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}
