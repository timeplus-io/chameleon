package proton

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type DataRow []interface{}

type IngestData struct {
	Columns []string  `json:"columns"`
	Data    []DataRow `json:"data"`
}

type IngestPayload struct {
	Data   IngestData `json:"data"`
	Stream string     `json:"stream"`
}
