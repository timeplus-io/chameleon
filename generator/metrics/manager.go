package metrics

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/timeplus-io/chameleon/generator/log"
	timeplus "github.com/timeplus-io/go-client/client"
	timeplusMetrics "github.com/timeplus-io/go-client/metrics"
)

type Manager struct {
	metrics         sync.Map
	timeplusMetrics *timeplusMetrics.Metrics
}

type Metric struct {
	records []MetricRecord
}

type MetricRecord struct {
	value float64
	time  int64
}

func NewMetric() *Metric {
	return &Metric{
		records: make([]MetricRecord, 0),
	}
}

func NewManager(timeplusAddress string, timeplusAPIkey string) *Manager {
	timeplusClient := timeplus.NewCient(timeplusAddress, timeplusAPIkey)
	var m *timeplusMetrics.Metrics
	m, err := timeplusMetrics.NewMetrics("generator", []string{"name"}, []string{"value", "time"}, timeplusClient)
	if err != nil {
		fmt.Printf("failed to create metric, %s\n", err)
	}

	return &Manager{
		metrics:         sync.Map{},
		timeplusMetrics: m,
	}
}

func (m *Manager) Add(name string) {
	if _, ok := m.metrics.Load(name); !ok {
		metric := NewMetric()
		m.metrics.Store(name, metric)
	} else {
		log.Logger().Errorf("metric %s already exist", name)
	}
}

func (m *Manager) Observe(name string, value float64, tags map[string]interface{}) {
	// observer in dedicated go routine
	go func() {
		if metric, ok := m.metrics.Load(name); !ok {
			log.Logger().Errorf("metric %s doesnot exist", name)
		} else {
			record := MetricRecord{
				value: value,
				time:  time.Now().UnixMilli(),
			}
			metric.(*Metric).records = append(metric.(*Metric).records, record)
		}

		m.timeplusMetrics.Observe("timeplus", "test", []any{name}, []any{value, float64(time.Now().UnixMilli())}, tags)
	}()
}

func (m *Manager) Save(namesapce string) {
	log.Logger().Infof("save result to fle")
	m.metrics.Range(func(key, value interface{}) bool {
		metricName := key.(string)
		metric := value.(*Metric)
		m.save(namesapce, metricName, metric)
		return true
	})
}

func (m *Manager) save(namesapce string, name string, metric *Metric) {
	ts := time.Now().Unix()
	filePath := fmt.Sprintf("%s_%s_report_%d.csv", namesapce, name, ts)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Logger().Fatalf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)

	header := fmt.Sprintf("time,%s", name)
	datawriter.WriteString(header + "\n")

	for _, record := range metric.records {
		row := fmt.Sprintf("%d,%f", record.time, record.value)
		datawriter.WriteString(row + "\n")
	}

	datawriter.Flush()
	m.timeplusMetrics.Flush()
	file.Close()
}

func (m *Manager) Flush() {
	m.timeplusMetrics.Flush()
}
