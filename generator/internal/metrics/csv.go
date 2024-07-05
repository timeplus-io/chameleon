package metrics

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/timeplus-io/chameleon/generator/internal/log"
)

type CSVManager struct {
	metrics sync.Map
}

func NewCSVMetricManager() *CSVManager {
	return &CSVManager{
		metrics: sync.Map{},
	}
}

func (m *CSVManager) Add(name string) {
	if _, ok := m.metrics.Load(name); !ok {
		metric := NewMetric()
		m.metrics.Store(name, metric)
	} else {
		log.Logger().Errorf("metric %s already exist", name)
	}
}

func (m *CSVManager) Observe(name string, value float64, tags map[string]interface{}) {
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
	}()
}

func (m *CSVManager) Save(namesapce string) {
	log.Logger().Infof("save result to fle")
	m.metrics.Range(func(key, value interface{}) bool {
		metricName := key.(string)
		metric := value.(*Metric)
		m.save(namesapce, metricName, metric)
		return true
	})
}

func (m *CSVManager) save(namesapce string, name string, metric *Metric) {
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
	file.Close()
}

func (m *CSVManager) Flush() {
}
