package common

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type Event map[string]interface{}

func (e Event) String() string {
	kv := make([]string, len(e))
	index := 0
	for k, v := range e {
		kv[index] = fmt.Sprintf("%s=%v", k, v)
		index += 1
	}
	return strings.Join(kv, ",")
}

func (e Event) GetHeader() []string {
	header := make([]string, len(e))
	index := 0
	for k := range e {
		header[index] = k
		index++
	}
	return header
}

func (e Event) GetRow(header []string) []interface{} {
	row := make([]interface{}, len(header))
	for i, h := range header {
		// in case the event value is a map/array, turn it into string
		if reflect.ValueOf(e[h]).Kind() == reflect.Map || reflect.ValueOf(e[h]).Kind() == reflect.Slice {
			value, _ := json.Marshal(e[h])
			row[i] = string(value)
		} else {
			if v, exist := e[h]; exist {
				row[i] = v
			}
			// keep the row empty in case the field not found
		}
	}
	return row
}

func ToEvents(headers []string, rows [][]interface{}) []Event {
	events := make([]Event, len(rows))
	for index, row := range rows {
		event := make(Event)
		for i, cell := range row {
			event[headers[i]] = cell
		}
		events[index] = event
	}
	return events
}
