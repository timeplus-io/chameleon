package job

import (
	"github.com/google/uuid"
	"github.com/timeplus-io/chameleon/generator/common"
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
	source source.Source
	sinks  []sink.Sink
}

func NewJob(config JobConfiguration) (*Job, error) {
	source, err := source.NewGenarator(config.Source)
	if err != nil {
		return nil, err
	}

	sinks := make([]sink.Sink, len(config.Sinks))
	for index, sinkConfig := range config.Sinks {
		sinks[index] = sink.CreateSink(sinkConfig)
	}

	return CreateJob(config.Name, source, sinks), nil
}

func CreateJob(name string, source source.Source, sinks []sink.Sink) *Job {
	id := uuid.New().String()
	job := &Job{
		Id:     id,
		Name:   name,
		Status: STATUS_INIT,
		source: source,
		sinks:  sinks,
	}

	// initialize all sinks with fields defineid in source
	fields := job.source.GetFields()
	for _, sink := range job.sinks {
		sink.Init(fields)
	}

	return job
}

func (j *Job) ID() string {
	return j.Id
}

func (j *Job) Start() {
	j.source.Start()
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
					sink.Write(header, [][]interface{}{row})
				}
			}
		}()
	}
}

func (j *Job) Stop() {
	j.source.Stop()
	j.Status = STATUS_STOPPED
}
