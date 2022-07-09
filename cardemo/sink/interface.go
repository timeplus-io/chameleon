package sink

import "github.com/timeplus-io/chameleon/cardemo/common"

type Sink interface {
	Init() error
	InitCars(cars []*common.DimCar) error
	InitUsers(users []*common.DimUser) error
	Send(event map[string]any, stream string, timeCol string) error
}

type JobConfig struct {
	SinkConfigs []SinkConfig `json:"sinks"`
}

type SinkConfig struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}
