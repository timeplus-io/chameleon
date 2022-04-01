package observer

type Observer interface {
	Observe() error
	Stop()
}

type Configuration struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}
