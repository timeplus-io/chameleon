package test_test

import (
	"time"

	"github.com/timeplus-io/chameleon/generator/job"
	"github.com/timeplus-io/chameleon/generator/plugins/materialize"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Materialize", func() {

	BeforeEach(func() {
		materialize.Init()
	})

	Describe("Materialize test", func() {

		It("create Materialize sink job and run it", func() {
			jobConfig := job.JobConfiguration{
				Name:   "test",
				Source: source.DefaultConfiguration(),
				Sinks: []sink.Configuration{
					{
						Type: materialize.MATERIALIZE_SINK_TYPE,
						Properties: map[string]interface{}{
							"hec_token": "abcd1234",
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
			time.Sleep(5 * time.Second)
			njob.Stop()
		})

		It("create materialize latency observer", func() {
			properties := map[string]interface{}{
				"metric": "latency",
				"query":  `select value, time from test where value=9`,
			}
			ob, err := materialize.NewMaterializeObserver(properties)
			Expect(err).ShouldNot(HaveOccurred())

			go func() {
				err = ob.Observe()
				Expect(err).ShouldNot(HaveOccurred())
			}()

			time.Sleep(20 * time.Second)
			ob.Stop()
		})

		It("create materialize throghput observer", func() {
			properties := map[string]interface{}{
				"metric": "throughput",
				"query":  "SELECT '1' as a, count(*) FROM test WHERE mz_logical_timestamp()  < timestamp + 1000 GROUP BY a",
			}
			ob, err := materialize.NewMaterializeObserver(properties)
			Expect(err).ShouldNot(HaveOccurred())

			go func() {
				err = ob.Observe()
				Expect(err).ShouldNot(HaveOccurred())
			}()

			time.Sleep(20 * time.Second)
			ob.Stop()
		})

		It("create materialize availability observer", func() {
			properties := map[string]interface{}{
				"metric": "availability",
				"query":  "SELECT count(*) FROM test",
			}
			ob, err := materialize.NewMaterializeObserver(properties)
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
