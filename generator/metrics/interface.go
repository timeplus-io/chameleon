package metrics

type Metrics interface {
	Add(name string)
	Observe(name string, value float64, tags map[string]interface{})
	Save(namespace string)
	Flush()
}
