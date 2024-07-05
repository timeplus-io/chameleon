package proton

import (
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/observer"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
)

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        ProtonSinkType,
		Constructor: NewProtonSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", ProtonSinkType)

	obItem := observer.ObRegItem{
		Name:        ProtonOBType,
		Constructor: NewProtonObserver,
	}
	observer.Register(obItem)
	log.Logger().Infof("observer plugin %s has been registered", ProtonOBType)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}
