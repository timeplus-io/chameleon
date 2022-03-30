package test_test

import (
	"time"

	"github.com/timeplus-io/chameleon/generator/job"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Job", func() {

	BeforeEach(func() {
	})

	Describe("Job test", func() {

		FIt("create job and run it", func() {
			config := source.DefaultConfiguration()
			generator, err := source.NewGenarator(config)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(generator).ShouldNot(BeNil())

			console := sink.NewConsoleSink()
			Expect(console).ShouldNot(BeNil())

			job := job.NewJob("test job", generator, []sink.Sink{console})

			job.Start()

			time.Sleep(3 * time.Second)

			job.Stop()
		})
	})
})
