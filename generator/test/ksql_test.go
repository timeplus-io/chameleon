package test_test

import (
	"time"

	"github.com/timeplus-io/chameleon/generator/internal/job"
	"github.com/timeplus-io/chameleon/generator/internal/plugins/ksql"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test KSQL", func() {
	BeforeEach(func() {
		ksql.Init()
	})

	Describe("KSQL test", func() {
		It("create Ksql sink job and run it", func() {
			jobConfig := job.JobConfiguration{
				Name:   "test",
				Source: source.DefaultConfiguration(),
				Sinks: []sink.Configuration{
					{
						Type: ksql.KSQL_SINK_TYPE,
						Properties: map[string]interface{}{
							"host": "localhost",
						},
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

		It("create Ksql sink job with writing to broker and run it", func() {
			jobConfig := job.JobConfiguration{
				Name:   "test",
				Source: source.DefaultConfiguration(),
				Sinks: []sink.Configuration{
					{
						Type: ksql.KSQL_SINK_TYPE,
						Properties: map[string]interface{}{
							"host":       "localhost",
							"use_broker": true,
							"brokers":    "localhost:9092",
						},
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

		It("create ksql latency observer", func() {
			properties := map[string]interface{}{
				"metric": "latency",
				"query":  `select time, value from test where value=9`,
			}
			ob, err := ksql.NewKSQLObserver(properties)
			Expect(err).ShouldNot(HaveOccurred())

			go func() {
				err = ob.Observe()
				Expect(err).ShouldNot(HaveOccurred())
			}()

			time.Sleep(20 * time.Second)
			ob.Stop()
		})

		FIt("create ksql throughput observer", func() {
			properties := map[string]interface{}{
				"metric": "throughput",
				"query":  `SELECT count(*), g FROM test GROUP BY g`,
			}
			ob, err := ksql.NewKSQLObserver(properties)
			Expect(err).ShouldNot(HaveOccurred())

			go func() {
				err = ob.Observe()
				Expect(err).ShouldNot(HaveOccurred())
			}()

			time.Sleep(20 * time.Second)
			ob.Stop()
		})
	})
})
