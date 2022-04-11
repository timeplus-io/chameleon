package test_test

import (
	"time"

	"github.com/timeplus-io/chameleon/generator/job"
	"github.com/timeplus-io/chameleon/generator/plugins/kafka"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Splunk", func() {

	BeforeEach(func() {
		kafka.Init()
	})

	Describe("Kafka test", func() {
		It("create kafka sink job and run it", func() {
			jobConfig := job.JobConfiguration{
				Name:   "test",
				Source: source.DefaultConfiguration(),
				Sinks: []sink.Configuration{
					{
						Type:       kafka.KAFKA_SINK_TYPE,
						Properties: map[string]interface{}{},
					},
				},
			}

			manager := job.NewJobManager()

			njob, err := manager.CreateJob(jobConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(njob).ShouldNot(BeNil())
			Expect(njob.Status).Should(Equal(job.STATUS_INIT))

			njob.Start()
			time.Sleep(10 * time.Second)
			njob.Stop()
		})

		FIt("create kafka latency observer", func() {
			properties := map[string]interface{}{
				"metric": "latency",
				"topic":  "test",
			}
			kafkaOb, err := kafka.NewKafkaObserver(properties)
			Expect(err).ShouldNot(HaveOccurred())

			go func() {
				err = kafkaOb.Observe()
				Expect(err).ShouldNot(HaveOccurred())
			}()

			time.Sleep(30 * time.Second)
			kafkaOb.Stop()
		})
	})
})
