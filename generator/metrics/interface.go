package metrics

type Metrics interface {
	Add(name string)
	Observe(name string, value float64)
	Save(namespace string)
}
