package job

import (
	"fmt"
	"sync"

	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"
)

type JobConfiguration struct {
	Name     string                 `json:"name"`
	Source   source.Configuration   `json:"source,omitempty"`
	Sinks    []sink.Configuration   `json:"sinks,omitempty"`
	Observer observer.Configuration `json:"observer,omitempty"`
}

type JobManager struct {
	jobs sync.Map
}

func NewJobManager() *JobManager {
	return &JobManager{
		jobs: sync.Map{},
	}
}

func (m *JobManager) CreateJob(config JobConfiguration) (*Job, error) {
	job, err := NewJob(config)
	if err != nil {
		return nil, err
	}
	m.jobs.Store(job.ID(), job)
	return job, nil
}

func (m *JobManager) ListJob() []*Job {
	result := make([]*Job, 0)
	m.jobs.Range(func(key, value interface{}) bool {
		job := value.(*Job)
		result = append(result, job)
		return true
	})
	return result
}

func (m *JobManager) GetJob(id string) (*Job, error) {
	job, ok := m.jobs.Load(id)
	if !ok {
		return nil, fmt.Errorf("%s job does not exist", id)
	}
	return job.(*Job), nil
}

func (m *JobManager) DeleteJob(id string) error {
	_, ok := m.jobs.Load(id)
	if !ok {
		return fmt.Errorf("%s job does not exist", id)
	}
	m.jobs.Delete(id)
	return nil
}

func (m *JobManager) StartJob(id string) error {
	job, ok := m.jobs.Load(id)
	if !ok {
		return fmt.Errorf("%s job does not exist", id)
	}
	job.(*Job).Start()
	return nil
}

func (m *JobManager) StopJob(id string) error {
	job, ok := m.jobs.Load(id)
	if !ok {
		return fmt.Errorf("%s job does not exist", id)
	}
	job.(*Job).Stop()
	return nil
}
