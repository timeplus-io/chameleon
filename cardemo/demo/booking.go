package demo

import (
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/timeplus-io/chameleon/cardemo/log"
)

type Booking struct {
	ID          string    `mapstructure:"bid"`
	Time        time.Time `mapstructure:"time"`
	UID         string    `mapstructure:"uid"`
	CID         string    `mapstructure:"cid"`
	BookingTime time.Time `mapstructure:"booking_time"`
	Action      string    `mapstructure:"action"`
	Expire      time.Time `mapstructure:"expire"`
}

func NewBooking(user *User, car *Car) *Booking {
	bid := fmt.Sprintf("b%05d", BidCounter)
	BidCounter++

	booking := Booking{
		ID:          bid,
		Time:        time.Now().UTC(),
		UID:         user.ID,
		CID:         car.ID,
		Action:      "add",
		BookingTime: time.Now().UTC(),
		Expire:      time.Now().UTC().Add(BOOKING_EXPIRE_TIME * time.Minute),
	}

	return &booking
}

func (b *Booking) Act(action string) {
	b.Action = action
	b.Time = time.Now().UTC()
}

func (b *Booking) Event() map[string]any {
	var event map[string]any
	err := mapstructure.Decode(b, &event)

	if err != nil {
		log.Logger().Fatal("failed to decode booking event")
	}

	event["time"] = b.Time
	event["expire"] = b.Expire
	event["booking_time"] = b.BookingTime
	return event
}
