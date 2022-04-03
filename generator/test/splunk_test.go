package test_test

import (
	"time"

	"github.com/timeplus-io/chameleon/generator/job"
	"github.com/timeplus-io/chameleon/generator/plugins/splunk"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Splunk", func() {

	BeforeEach(func() {
		//console.Init()
	})

	Describe("Splunk test", func() {

		It("create splunk sink job and run it", func() {
			jobConfig := job.JobConfiguration{
				Name:   "test",
				Source: source.DefaultConfiguration(),
				Sinks: []sink.Configuration{
					{
						Type: splunk.SPLUNK_SINK_TYPE,
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

		FIt("create splunk search observer", func() {
			properties := map[string]interface{}{
				"search": `search index=main source="my_source" value=90000 | eval eventtime=_time | eval indextime=_indextime`,
			}

			splunkOb, err := splunk.NewSplunkObserver(properties)
			Expect(err).ShouldNot(HaveOccurred())

			err = splunkOb.Observe()
			Expect(err).ShouldNot(HaveOccurred())
			splunkOb.Stop()
		})
	})
})
