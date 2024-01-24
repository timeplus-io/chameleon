package metrics

type MetricRecord struct {
	value float64
	time  int64
}

type Metric struct {
	records []MetricRecord
}
