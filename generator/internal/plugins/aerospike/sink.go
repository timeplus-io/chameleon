package aerospike

import (
	"fmt"

	"github.com/timeplus-io/chameleon/generator/internal/common"
	"github.com/timeplus-io/chameleon/generator/internal/log"
	"github.com/timeplus-io/chameleon/generator/internal/sink"
	"github.com/timeplus-io/chameleon/generator/internal/utils"

	aero "github.com/aerospike/aerospike-client-go/v7"
)

const AEROSPIKE_SINK_TYPE = "aerospike"

type AeroSpikeSink struct {
	client       *aero.Client
	namespace    string
	set          string
	successwrite int
	failedwrite  int
}

func NewAeroSpikeSink(properties map[string]interface{}) (sink.Sink, error) {
	host, err := utils.GetWithDefault(properties, "host", "localhost")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	port, err := utils.GetIntWithDefault(properties, "port", 3000)
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	namespace, err := utils.GetWithDefault(properties, "namespace", "test")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	set, err := utils.GetWithDefault(properties, "set", "default")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	if client, err := aero.NewClient(host, port); err != nil {
		return nil, err
	} else {
		return &AeroSpikeSink{
			client:       client,
			namespace:    namespace,
			set:          set,
			successwrite: 0,
			failedwrite:  0,
		}, nil
	}
}

func (s *AeroSpikeSink) Init(name string, fields []common.Field) error {
	return nil
}

func (s *AeroSpikeSink) Write(headers []string, rows [][]interface{}, index int) error {
	events := common.ToEvents(headers, rows)
	for _, event := range events {
		key := utils.RandStringBytes(8)

		if aKey, err := aero.NewKey(s.namespace, s.set, key); err != nil {
			log.Logger().Errorf("failed to create key %s", err)
			s.failedwrite += 1
			continue
		} else {
			bins := aero.BinMap(event)
			if err = s.client.Put(nil, aKey, bins); err != nil {
				log.Logger().Errorf("failed to write key %s", err)
				s.failedwrite += 1
			} else {
				s.successwrite += 1
			}
		}

	}
	return nil
}

func (s *AeroSpikeSink) GetStats() *sink.Stats {
	return &sink.Stats{
		SuccessWrite: 0,
		FailedWrite:  0,
	}
}
