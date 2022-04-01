package job

import (
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
	Id       string    `json:"id"`
	Name     string    `json:"name"`
	Status   JobStatus `json:"status"`
	source   source.Source
	sinks    []sink.Sink
	observer observer.Observer
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
		return nil, err
	}

	return CreateJob(config.Name, source, sinks, obs), nil
}

func CreateJob(name string, source source.Source, sinks []sink.Sink, obs observer.Observer) *Job {
	id := uuid.New().String()
	job := &Job{
		Id:       id,
		Name:     name,
		Status:   STATUS_INIT,
		source:   source,
		sinks:    sinks,
		observer: obs,
	}

	// initialize all sinks with fields defineid in source
	fields := job.source.GetFields()
	for _, sink := range job.sinks {
		sink.Init(name, fields)
	}

	return job
}

func (j *Job) ID() string {
	return j.Id
}

func (j *Job) Start() {
	j.source.Start()
	go j.observer.Observe() // start observer go routine
	j.Status = STATUS_RUNNING
	for _, stream := range j.source.GetStreams() {
		go func() {
			// write one event to each sink
			// should enable batch later
			for item := range stream.Observe() {
				event := item.V.(common.Event)
				header := event.GetHeader()
				row := event.GetRow(header)
				for _, sink := range j.sinks {
					data := make([][]interface{}, 1)
					data[0] = row
					if err := sink.Write(header, data); err != nil {
						log.Logger().Errorf("failed to write event : %w ", err)
					}
				}
			}
		}()
	}
}

func (j *Job) Stop() {
	j.source.Stop()
	j.Status = STATUS_STOPPED
}
