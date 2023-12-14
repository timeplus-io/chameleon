package materialize

import (
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/observer"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
)

func init() {
	sinkItem := sink.SinkRegItem{
		Name:        MATERIALIZE_SINK_TYPE,
		Constructor: NewMaterializeSink,
	}
	sink.Register(sinkItem)
	log.Logger().Infof("sink plugin %s has been registered", MATERIALIZE_SINK_TYPE)

	obItem := observer.ObRegItem{
		Name:        MATERIALIZE_OB_TYPE,
		Constructor: NewMaterializeObserver,
	}
	observer.Register(obItem)
	log.Logger().Infof("observer plugin %s has been registered", MATERIALIZE_OB_TYPE)
}

func Init() {
	// this function did nothing, just to make sure the module is initialized
	// consider plugin https://pkg.go.dev/plugin to make sink/source plugable
}
