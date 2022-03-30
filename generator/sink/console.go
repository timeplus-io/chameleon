package sink

import (
	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
)

type Console struct{}

func NewConsoleSink() *Console {
	return &Console{}
}

func (s *Console) Write(headers []string, rows [][]interface{}) error {
	log.Logger().Infof("Write one event to console %v:%v", headers, rows)
	return nil
}

func (s *Console) Init(fields []common.Field) error {
	return nil
}
