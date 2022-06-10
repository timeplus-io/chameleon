package rocketmq

import (
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/sink"
)

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        ROCKETMQ_SINK_TYPE,
		Constructor: NewRocketMQSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", ROCKETMQ_SINK_TYPE)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}
