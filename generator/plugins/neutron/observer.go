package neutron

import (
	"fmt"

	"github.com/timeplus-io/chameleon/generator/log"
	"github.com/timeplus-io/chameleon/generator/observer"
	"github.com/timeplus-io/chameleon/generator/utils"
)

const NEUTRON_OB_TYPE = "neutron"

type NeutronObserver struct {
	server     *NeutronServer
	query      string
	timeColumn string
}

func NewNeutronObserver(properties map[string]interface{}) (observer.Observer, error) {
	address, err := utils.GetWithDefault(properties, "address", "http://localhost:8000")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	query, err := utils.GetWithDefault(properties, "query", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	timeColumn, err := utils.GetWithDefault(properties, "time_column", "")
	if err != nil {
		return nil, fmt.Errorf("invalid properties : %w", err)
	}

	return &NeutronObserver{
		server:     NewNeutronServer(address),
		query:      query,
		timeColumn: timeColumn,
	}, nil
}

func (o *NeutronObserver) Observe() error {
	log.Logger().Infof("start observing")

	resultStream, err := o.server.QueryStream(o.query)
	if err != nil {
		return err
	}

	for item := range resultStream.Observe() {
		log.Logger().Infof("observe one result %v", item)
	}
	log.Logger().Infof("stop observing")
	return nil
}
