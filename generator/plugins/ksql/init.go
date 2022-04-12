package ksql

import (
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/sink"
)

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        KSQL_SINK_TYPE,
		Constructor: NewKSQLSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", KSQL_SINK_TYPE)

	obItem := observer.ObRegItem{
		Name:        KSQL_OB_TYPE,
		Constructor: NewKSQLObserver,
	}
	observer.Register(obItem)
	log.Logger().Infof("observer plugin %s has been registered", KSQL_OB_TYPE)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}
