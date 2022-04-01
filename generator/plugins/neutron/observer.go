package neutron

import (
	"fmt"
	"time"

	"github.com/timeplus-io/chameleon/generator/common"
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

	layout := "2006-01-02T15:04:05.000-07:00" // should be a config
	for item := range resultStream.Observe() {
		event := item.V.(common.Event)
		t, err := time.Parse(layout, event[o.timeColumn].(string))
		if err != nil {
			continue
		}
		log.Logger().Debugf("observe one result %v", item)
		log.Logger().Infof("observe latency %v", time.Until(t))
	}
	log.Logger().Infof("stop observing")
	return nil
}

func (o *NeutronObserver) Stop() {
}
