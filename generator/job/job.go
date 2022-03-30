package job

import (
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
	Name   string        `json:"name"`
	Source source.Source `json:"source"`
	Sinks  []sink.Sink   `json:"sinks"`
	Status JobStatus     `json:"status"`
}

func NewJob(name string, source source.Source, sinks []sink.Sink) *Job {
	job := &Job{
		Name:   name,
		Source: source,
		Sinks:  sinks,
		Status: STATUS_INIT,
	}

	// initialize all sinks with fields defineid in source
	fields := job.Source.GetFields()
	for _, sink := range job.Sinks {
		sink.Init(fields)
	}

	return job
}

func (j *Job) Start() {
	j.Source.Start()
	for _, stream := range j.Source.GetStreams() {
		go func() {
			// write one event to each sink
			// should enable batch later
			for item := range stream.Observe() {
				event := item.V.(common.Event)
				header := event.GetHeader()
				row := event.GetRow(header)
				for _, sink := range j.Sinks {
					sink.Write(header, [][]interface{}{row})
				}
			}
		}()
	}
}

func (j *Job) Stop() {
	j.Source.Stop()
}
