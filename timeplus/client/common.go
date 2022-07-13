package client

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

type DataRow []any

func (r DataRow) MarshalJSON() ([]byte, error) {
	var result []string

	for _, col := range r {

		switch v := col.(type) {
		// In case of divide by zero, Proton will return `+Inf`. There is no such type for JSON so here we simply marshal it into `null`.
		// Noted that we haven't seen any case where Proton returns `NaN` so far.
		case float64:
			if math.IsInf(v, 1) || math.IsInf(v, -1) || math.IsNaN(v) {
				result = append(result, "null")
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
