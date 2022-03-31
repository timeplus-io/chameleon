package sink

import (
	"fmt"
)

type SinkConstructor func(properties map[string]interface{}) (Sink, error)

type SinkRegItem struct {
	Name        string
	Constructor SinkConstructor
}

var (
	sinkRegistry map[string]SinkRegItem
)

func init() {
	sinkRegistry = make(map[string]SinkRegItem)
}

// The sink will register itself in `init`. During that the the logger may not be inited. So here we'd better not log anything
func Register(item SinkRegItem) {
	if _, exist := sinkRegistry[item.Name]; exist {
		panic(fmt.Errorf("item has already been registered"))
	}

	sinkRegistry[item.Name] = item
}

func CreateSink(config Configuration) (Sink, error) {
	constructor := sinkRegistry[config.Type].Constructor
	return constructor(config.Properties)
}
func GetConstructor(sinkName string) SinkConstructor {
	return sinkRegistry[sinkName].Constructor
}

func ListRegisteredSinkTypes() []string {
	keys := make([]string, 0)
	for k := range sinkRegistry {
		keys = append(keys, k)
	}
	return keys
}
