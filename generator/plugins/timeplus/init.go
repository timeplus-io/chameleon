package timeplus

import (
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/sink"
)

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        TimeplusSinkType,
		Constructor: NewTimeplusSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", TimeplusSinkType)

	obItem := observer.ObRegItem{
		Name:        TimeplusOBType,
		Constructor: NewTimeplusObserver,
	}
	observer.Register(obItem)
	log.Logger().Infof("observer plugin %s has been registered", TimeplusOBType)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}
