package client_test

import (
	"github.com/timeplus-io/chameleon/timeplus/client"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("timeplus API Test", func() {
	var timeplusClient *client.TimeplusClient

	BeforeEach(func() {
		timeplusClient = client.NewCient("http://localhost:8000", "")
	})

	AfterEach(func() {
	})

	Describe("Test API", func() {
		It("should ping server", func() {
			_, err := timeplusClient.ListStream()
			Expect(err).ShouldNot(HaveOccurred())
		})

	})

})
