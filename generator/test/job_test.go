package test_test

import (
	"time"

	"github.com/timeplus-io/chameleon/generator/internal/job"
	"github.com/timeplus-io/chameleon/generator/internal/observer"
	"github.com/timeplus-io/chameleon/generator/internal/plugins/console"
	"github.com/timeplus-io/chameleon/generator/internal/plugins/timeplus"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Job", func() {

	BeforeEach(func() {
		console.Init()
		timeplus.Init()
	})

	Describe("Job test", func() {

		It("create job and run it", func() {
			config := source.DefaultConfiguration()
			generator, err := source.NewGenarator(config)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(generator).ShouldNot(BeNil())

			console, err := console.NewConsoleSink(nil)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(console).ShouldNot(BeNil())

			job := job.CreateJob("test job", generator, []sink.Sink{console}, nil, 0)

			job.Start()
			time.Sleep(3 * time.Second)
			job.Stop()
		})

		It("job crud", func() {
			jobConfig := job.JobConfiguration{
				Name:   "test job",
				Source: source.DefaultConfiguration(),
				Sinks: []sink.Configuration{
					{
						Type:       "console",
						Properties: map[string]interface{}{},
					},
				},
			}

			manager := job.NewJobManager()

			jobs := manager.ListJob()
			Expect(len(jobs)).Should(Equal(0))

			ajob, err := manager.CreateJob(jobConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ajob).ShouldNot(BeNil())
			Expect(ajob.Status).Should(Equal(job.STATUS_INIT))

			bjob, err := manager.GetJob(ajob.Id)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ajob).Should(Equal(bjob))

			jobs = manager.ListJob()
			Expect(len(jobs)).Should(Equal(1))

			manager.StartJob(ajob.Id)
			Expect(ajob.Status).Should(Equal(job.STATUS_RUNNING))

			manager.StopJob(ajob.Id)
			Expect(ajob.Status).Should(Equal(job.STATUS_STOPPED))

			err = manager.DeleteJob(ajob.Id)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = manager.GetJob(ajob.Id)
			Expect(err).Should(HaveOccurred())

			jobs = manager.ListJob()
			Expect(len(jobs)).Should(Equal(0))
		})

		It("test ob only job", func() {
			jobConfig := job.JobConfiguration{
				Name: "test job",
				Observer: observer.Configuration{
					Type:       "neutron",
					Properties: map[string]interface{}{},
				},
			}

			manager := job.NewJobManager()
			ajob, err := manager.CreateJob(jobConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ajob).ShouldNot(BeNil())
			Expect(ajob.Status).Should(Equal(job.STATUS_INIT))

			manager.StartJob(ajob.Id)
			Expect(ajob.Status).Should(Equal(job.STATUS_RUNNING))

			time.Sleep(3 * time.Second)
			manager.StopJob(ajob.Id)
			Expect(ajob.Status).Should(Equal(job.STATUS_STOPPED))
		})
	})
})
