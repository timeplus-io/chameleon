package sinks

import (
	"github.com/timeplus-io/chameleon/dataloader/models"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Sink interface {
	LoadData()
	Stop()
}

var globalSinkFactory = make(map[string]func(*models.Config, *zap.Logger) (Sink, error))

func RegisterSink(sinkType string, f func(*models.Config, *zap.Logger) (Sink, error)) {
	globalSinkFactory[sinkType] = f
}

func NewSink(c *models.Config, logger *zap.Logger) (Sink, error) {
	f, ok := globalSinkFactory[c.Sink.Type]
	if ok {
		return f(c, logger)
	}
	return nil, errors.Errorf("invalid sink type=%s, no corresponding sink factory for it", c.Sink.Type)
}
