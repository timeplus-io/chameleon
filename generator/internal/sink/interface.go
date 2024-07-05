package sink

import "github.com/timeplus-io/chameleon/generator/internal/common"

type Stats struct {
	SuccessWrite int
	FailedWrite  int
}

type Sink interface {
	Write(headers []string, rows [][]interface{}, index int) error
	Init(name string, fields []common.Field) error
	GetStats() *Stats
}

type Configuration struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}
