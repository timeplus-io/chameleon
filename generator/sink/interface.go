package sink

import "github.com/timeplus-io/chameleon/generator/common"

type Sink interface {
	Write(headers []string, rows [][]interface{}) error
	Init(name string, fields []common.Field) error
}

type Configuration struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}
