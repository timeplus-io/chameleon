package demo

import (
	"time"

	"github.com/timeplus-io/chameleon/cardemo/log"

	"github.com/mitchellh/mapstructure"
)

type Pay struct {
	Time   time.Time `mapstructure:"time"`
	Uid    string    `mapstructure:"uid"`
	Amount float64   `mapstructure:"amount"`
	Note   string    `mapstructure:"note"`
}

func NewPay(uid string, amount float64, note string) *Pay {
	pay := Pay{
		Time:   time.Now().UTC(),
		Uid:    uid,
		Amount: amount,
		Note:   note,
	}
	return &pay
}

func (p *Pay) Event() map[string]any {
	var event map[string]any
	err := mapstructure.Decode(p, &event)

	if err != nil {
		log.Logger().Fatal("failed to decode car event")
	}
	event["time"] = p.Time
	return event
}
