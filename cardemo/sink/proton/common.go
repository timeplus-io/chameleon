package proton

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
} // @name Column

// For Proton query response
type ResponseDataRow []any

// For ingest path
type IngestDataRow []any

type IngestData struct {
	Columns []string        `json:"columns"`
	Data    []IngestDataRow `json:"data"`
} // @name IngestData

// We need to explictly separate Ingest and Response data row since we have to overwrite the marshal behaviou for resposne data row only
func (r ResponseDataRow) MarshalJSON() ([]byte, error) {
	var result []string

	for _, col := range r {

		switch v := col.(type) {
		// In case of divide by zero, Proton will return `+Inf`. There is no such type for JSON so here we simply marshal it into `null`.
		// Noted that we haven't seen any case where Proton returns `NaN` so far.
		case float64:
			if math.IsInf(v, 1) {
				result = append(result, "\"inf\"")
				continue
			} else if math.IsInf(v, -1) {
				result = append(result, "\"-inf\"")
				continue
			} else if math.IsNaN(v) {
				result = append(result, "\"nan\"")
				continue
			}
		// A sample query `SELECT [1,2]` will lead Proton to return uint8 [1,2]. By default JSON marshaler will marshal []byte base64-encoded string.
		// Here we have to manually marshal it back to integer
		// https://pkg.go.dev/encoding/json#Marshal
		case []uint8:
			result = append(result, strings.Join(strings.Fields(fmt.Sprintf("%d", v)), ","))
			continue
		}

		b, err := json.Marshal(col)
		if err != nil {
			return nil, err
		}
		result = append(result, string(b))
	}

	return []byte(fmt.Sprintf("[%s]", strings.Join(result, ","))), nil
}
