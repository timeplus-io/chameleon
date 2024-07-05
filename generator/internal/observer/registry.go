package observer

import (
	"fmt"
)

type ObserverConstructor func(properties map[string]interface{}) (Observer, error)

type ObRegItem struct {
	Name        string
	Constructor ObserverConstructor
}

var (
	obRegistry map[string]ObRegItem
)

func init() {
	obRegistry = make(map[string]ObRegItem)
}

// The sink will register itself in `init`. During that the the logger may not be inited. So here we'd better not log anything
func Register(item ObRegItem) {
	if _, exist := obRegistry[item.Name]; exist {
		panic(fmt.Errorf("item has already been registered"))
	}

	obRegistry[item.Name] = item
}

func CreateObserver(config Configuration) (Observer, error) {
	if _, exist := obRegistry[config.Type]; !exist {
		return nil, fmt.Errorf("the observer %s doesnot exist", config.Type)
	}
	constructor := obRegistry[config.Type].Constructor
	return constructor(config.Properties)
}
func GetConstructor(obName string) ObserverConstructor {
	return obRegistry[obName].Constructor
}

func ListRegisteredSinkTypes() []string {
	keys := make([]string, 0)
	for k := range obRegistry {
		keys = append(keys, k)
	}
	return keys
}
