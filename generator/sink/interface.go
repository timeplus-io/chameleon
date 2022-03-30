package sink

import "github.com/timeplus-io/chameleon/generator/common"

type Sink interface {
	Write(headers []string, rows [][]interface{}) error
	Init(fields []common.Field) error
}
