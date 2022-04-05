package job

import (
	"bufio"
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/timeplus-io/chameleon/generator/common"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"
)

type JobStatus string

const (
	STATUS_INIT    JobStatus = "init"
	STATUS_RUNNING JobStatus = "running"
	STATUS_STOPPED JobStatus = "stopped"
	STATUS_FAILED  JobStatus = "failed"
)

type Job struct {
	Id     string    `json:"id"`
	Name   string    `json:"name"`
	Status JobStatus `json:"status"`

	source    source.Source
	sinks     []sink.Sink
	observer  observer.Observer
	jobWaiter sync.WaitGroup
	timeout   int
}

func LoadConfig(file string) (*JobConfiguration, error) {
	dat, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var payload JobConfiguration
	json.NewDecoder(bytes.NewBuffer(dat)).Decode(&payload)
	return &payload, nil
}

func SaveConfig(config JobConfiguration, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	defer f.Close()

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err = encoder.Encode(config); err != nil {
		return err
	}
	w.Flush()
	return nil
}

func NewJobFromFile(file string) (*Job, error) {
	jobConfig, err := LoadConfig(file)
	if err != nil {
		return nil, err
	}
	return NewJob(*jobConfig)
}

func NewJob(config JobConfiguration) (*Job, error) {
	source, err := source.NewGenarator(config.Source)
	if err != nil {
		return nil, err
	}

	sinks := make([]sink.Sink, len(config.Sinks))
	for index, sinkConfig := range config.Sinks {
		if sink, err := sink.CreateSink(sinkConfig); err != nil {
			return nil, err
		} else {
			sinks[index] = sink
		}
	}

	obs, err := observer.CreateObserver(config.Observer)
	if err != nil {
		log.Logger().Errorf("failed to create observer %s", config.Observer.Type)
	}

	return CreateJob(config.Name, source, sinks, obs, config.Timeout), nil
}

func CreateJob(name string, source source.Source, sinks []sink.Sink, obs observer.Observer, timeout int) *Job {
	id := uuid.New().String()
	job := &Job{
		Id:       id,
		Name:     name,
		Status:   STATUS_INIT,
		source:   source,
		sinks:    sinks,
		observer: obs,
		timeout:  timeout,
	}

	// initialize all sinks with fields defineid in source
	fields := job.source.GetFields()
	for _, sink := range job.sinks {
		sink.Init(name, fields) // todo : check init status here
	}

	return job
}

func (j *Job) ID() string {
	return j.Id
}

func (j *Job) Start() {
	startTime := time.Now()
	j.source.Start()

	if j.observer != nil {
		log.Logger().Infof("start observer")
		go j.observer.Observe() // start observer go routine
	}

	j.Status = STATUS_RUNNING

	streams := j.source.GetStreams()
	j.jobWaiter = sync.WaitGroup{}
	j.jobWaiter.Add(len(streams))

	for i, stream := range streams {
		time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
		go func(i int) {
			// write one event to each sink
			// should enable batch later
			for item := range stream.Observe() {
				events := item.V.([]common.Event)

				if len(events) == 0 {
					continue
				}

				header := events[0].GetHeader()
				data := make([][]interface{}, len(events))
				for index, event := range events {
					row := event.GetRow(header)
					data[index] = row
				}

				for _, sink := range j.sinks {
					if err := sink.Write(header, data, i); err != nil {
						log.Logger().Errorf("failed to write event : %w ", err)
					}
				}
			}
			j.jobWaiter.Done()
		}(i)
	}

	if j.timeout != 0 {
		for {
			time.Sleep(1 * time.Second)
			if -time.Until(startTime).Seconds() > float64(j.timeout) {
				log.Logger().Infof("timeout and exit data generating")
				j.Stop()
				break
			}
		}
	}
}

func (j *Job) Wait() {
	j.jobWaiter.Wait()
}

func (j *Job) Stop() {
	j.source.Stop()
	if j.observer != nil {
		j.observer.Stop()
	}
	j.Status = STATUS_STOPPED
}
