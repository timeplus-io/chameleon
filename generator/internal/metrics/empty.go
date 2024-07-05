package metrics

type EmptyManager struct {
}

func NewEmptyMetricManager() *EmptyManager {
	return &EmptyManager{}
}

func (m *EmptyManager) Add(name string) {
}

func (m *EmptyManager) Observe(name string, value float64, tags map[string]interface{}) {
}

func (m *EmptyManager) Save(namesapce string) {
}

func (m *EmptyManager) Flush() {
}
