package test_test

import (
	"time"

	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/source"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Generator", func() {

	BeforeEach(func() {
	})

	Describe("Generator test", func() {

		It("create generate and read data", func() {
			config := source.DefaultConfiguration()
			generator, err := source.NewGenarator(config)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(generator).ShouldNot(BeNil())

			generator.Start()

			time.Sleep(3 * time.Second)
			events := generator.Read()
			log.Logger().Infof("get events %v", events)
			Expect(events).ShouldNot(BeNil())

			events = generator.Read()
			log.Logger().Infof("get events %v", events)
			Expect(events).ShouldNot(BeNil())

			events = generator.Read()
			log.Logger().Infof("get events %v", events)
			Expect(events).ShouldNot(BeNil())

			generator.Stop()
		})
	})
})
