package common

type Metric struct {
	Name   string
	Values []MeausreValue
	Tags   []Tag
}

type Payload struct {
	Name      string
	Timestamp string
	Data      []interface{}
	Tags      []string
}

type MeausreValue struct {
	Name string
	Type string
}

type Tag struct {
	Name string
	Type string
}

func (m *Metric) GetMeasureNames() []string {
	header := []string{}
	for _, measure := range m.Values {
		header = append(header, measure.Name)
	}

	return header
}

func FindMetricByName(metrics []Metric, name string) *Metric {
	for index, metric := range metrics {
		if metric.Name == name {
			return &metrics[index]
		}
	}
	return nil
}

func FindMetricIndexByName(metrics []Metric, name string) int {
	for index, metric := range metrics {
		if metric.Name == name {
			return index
		}
	}
	return -1
}
