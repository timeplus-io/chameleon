package test_test

import (
	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/plugins/neutron"

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

		FIt("Test Insert Data", func() {
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
			err := server.CreateStream(streamDef)
			Expect(err).ShouldNot(HaveOccurred())

			ingestData := neutron.IngestPayload{
				Stream: streamDef.Name,
				Data: neutron.IngestData{
					Columns: []string{"a", "b"},
					Data: []neutron.DataRow{
						{1, "abc"},
						{2, "fgh"},
					},
				},
			}

			err = server.InsertData(ingestData)
			Expect(err).ShouldNot(HaveOccurred())

		})
	})
})
