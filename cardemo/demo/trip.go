package demo

import (
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/timeplus-io/chameleon/cardemo/log"
)

type Trip struct {
	ID             string    `mapstructure:"tid"`
	StartTime      time.Time `mapstructure:"start_time"`
	EndTime        time.Time `mapstructure:"end_time"`
	BookID         string    `mapstructure:"bid"`
	StartLongitude float64   `mapstructure:"start_lon"`
	StartLatitude  float64   `mapstructure:"start_lat"`
	EndLongitude   float64   `mapstructure:"end_lon"`
	EndLatitude    float64   `mapstructure:"end_lat"`
	Distance       float64   `mapstructure:"distance"`
	Amount         float64   `mapstructure:"amount"`
	PayType        string    `mapstructure:"pay_type"`

	startMileage float64
}

func NewTrip(bid string, longtitude float64, latitude float64, mileage float64) *Trip {
	tid := fmt.Sprintf("t%05d", TidCounter)
	TidCounter++

	trip := Trip{
		ID:             tid,
		StartTime:      time.Now().UTC(),
		BookID:         bid,
		StartLongitude: longtitude,
		StartLatitude:  latitude,
		startMileage:   mileage,
	}

	return &trip
}

func (t *Trip) End(longtitude float64, latitude float64, mileage float64) {
	t.EndTime = time.Now().UTC()
	t.EndLongitude = longtitude
	t.EndLatitude = latitude
	t.Distance = mileage - t.startMileage
}

func (t *Trip) Pay(price float64, payType string) {
	duration := float64(t.EndTime.Sub(t.StartTime) / time.Second)
	t.Amount = 1 + price*duration/60
	log.Logger().Debugf("Trip end with running duration %.2f minutes and fee $%.2f", duration/60, t.Amount)
	t.PayType = payType
}

func (t *Trip) Event() map[string]any {
	var event map[string]any
	err := mapstructure.Decode(t, &event)

	if err != nil {
		log.Logger().Fatal("failed to decode car event")
	}

	event["start_time"] = t.StartTime
	event["end_time"] = t.EndTime
	return event
}
