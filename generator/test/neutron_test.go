package test_test

import (
	"fmt"
	"time"

	"github.com/timeplus-io/chameleon/generator/job"
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/plugins/neutron"
	"github.com/timeplus-io/chameleon/generator/sink"
	"github.com/timeplus-io/chameleon/generator/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Job", func() {

	BeforeEach(func() {
		//console.Init()
	})

	Describe("Neutron test", func() {

		It("Create/List/Delete neutron stream", func() {
			address := "http://localhost:8000"
			server := neutron.NewNeutronServer(address)

			streamDef := neutron.StreamDef{
				Name: "testStream",
				Columns: []neutron.ColumnDef{
					{
						Name: "a",
						Type: "int",
					},
					{
						Name: "b",
						Type: "string",
					},
				},
			}

			server.DeleteStream(streamDef.Name)

			streams, err := server.ListStream()
			Expect(err).ShouldNot(HaveOccurred())

			log.Logger().Infof("get streams %v", streams)

			err = server.CreateStream(streamDef)
			Expect(err).ShouldNot(HaveOccurred())

			streams, err = server.ListStream()
			Expect(err).ShouldNot(HaveOccurred())
			log.Logger().Infof("get streams %v", streams)

			err = server.DeleteStream(streamDef.Name)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("Test Insert Data", func() {
			address := "http://localhost:8000"
			server := neutron.NewNeutronServer(address)

			streamDef := neutron.StreamDef{
				Name: "testStream",
				Columns: []neutron.ColumnDef{
					{
						Name: "number",
						Type: "int",
					},
					{
						Name: "time",
						Type: "datetime64(3)",
					},
				},
			}

			server.DeleteStream(streamDef.Name)

			time.Sleep(1 * time.Second)
			err := server.CreateStream(streamDef)
			Expect(err).ShouldNot(HaveOccurred())

			time.Sleep(1 * time.Second)
			ingestData := neutron.IngestPayload{
				Stream: streamDef.Name,
				Data: neutron.IngestData{
					Columns: []string{"number", "time"},
					Data: [][]interface{}{
						{1, "2022-03-31 23:58:56.344"},
						{2, "2022-03-31 23:58:56.344"},
					},
				},
			}
			err = server.InsertData(ingestData)
			Expect(err).ShouldNot(HaveOccurred())

			time.Sleep(3 * time.Second)
			sql := fmt.Sprintf("select * from table(%s)", streamDef.Name)
			result, err := server.QueryStream(sql)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result).ShouldNot(BeNil())

			for item := range result.Observe() {
				log.Logger().Infof("got one query result %v", item)
			}

			//server.DeleteStream(streamDef.Name)
		})

		It("create neutron sink job and run it", func() {
			jobConfig := job.JobConfiguration{
				Name:   "test",
				Source: source.DefaultConfiguration(),
				Sinks: []sink.Configuration{
					{
						Type: neutron.NEUTRON_SINK_TYPE,
						Properties: map[string]interface{}{
							"address": "http://localhost:8000",
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

		FIt("create neutron sink/ob job and run it", func() {
			jobConfig := job.JobConfiguration{
				Name: "test",
				Source: source.Configuration{
					BatchSize:   1,
					Concurrency: 1,
					Interval:    1,
					Fields: []source.Field{
						{
							Name:  "value",
							Type:  "int",
							Limit: []interface{}{float64(0), float64(10)},
						},
						{
							Name:            "time",
							Type:            "timestamp",
							TimestampFormat: "2006-01-02 15:04:05.000000",
						},
					},
				},
				Sinks: []sink.Configuration{
					{
						Type: neutron.NEUTRON_SINK_TYPE,
						Properties: map[string]interface{}{
							"address": "http://localhost:8000",
						},
					},
				},
				Observer: observer.Configuration{
					Type: neutron.NEUTRON_OB_TYPE,
					Properties: map[string]interface{}{
						"address":     "http://localhost:8000",
						"query":       "select * from test where value >9",
						"time_column": "time",
					},
				},
			}

			manager := job.NewJobManager()
			njob, err := manager.CreateJob(jobConfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(njob).ShouldNot(BeNil())
			Expect(njob.Status).Should(Equal(job.STATUS_INIT))

			njob.Start()
			time.Sleep(3 * time.Second)
			njob.Stop()
		})
	})
})
