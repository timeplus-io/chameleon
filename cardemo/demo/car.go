package demo

import (
	"sync"
	"time"

	fake "github.com/brianvoe/gofakeit/v6"
	"github.com/mitchellh/mapstructure"
	"github.com/reactivex/rxgo/v2"

	"github.com/timeplus-io/chameleon/cardemo/log"

	"github.com/spf13/viper"
)

type Car struct {
	ID            string  `mapstructure:"cid"`
	InUse         bool    `mapstructure:"in_use"`
	Longitude     float64 `mapstructure:"longitude"`
	Latitude      float64 `mapstructure:"latitude"`
	Speed         int     `mapstructure:"speed_kmh"`
	GasPercent    float64 `mapstructure:"gas_percent"`
	TotalDistance float64 `mapstructure:"total_km"`
	Locked        bool    `mapstructure:"locked"`
	//CurrentTime   time.Time `mapstructure:"time"`

	inService      bool
	booked         bool
	currentBooking *Booking
	price          float64
	currentUser    *User
	currentTrip    *Trip
	channels       AppChannels
	faker          *fake.Faker
	carRunInterval int
	idleDuration   int
	routes         *RouteList
	route          *Track

	lock sync.Mutex
}

func NewCar(cid string, inService bool, appChannels AppChannels, faker *fake.Faker, routes *RouteList) *Car {
	car := Car{
		ID:             cid,
		InUse:          false,
		GasPercent:     faker.Float64Range(REFILL_MIN, REFILL_MAX),
		TotalDistance:  faker.Float64Range(0.0, INITIAL_MILLAGE_MAX),
		Locked:         true,
		inService:      inService,
		booked:         false,
		price:          viper.GetFloat64("cardemo.car.price"),
		channels:       appChannels,
		carRunInterval: viper.GetInt("cardemo.car.update.interval"),
		idleDuration:   0,
		faker:          faker,
		routes:         routes,
		lock:           sync.Mutex{},
	}

	car.route = nil
	car.Latitude = 0
	car.Longitude = 0

	go (&car).StartSimulation()
	return &car
}

func (c *Car) StartSimulation() {
	// wait a random delay, so the car wont report status at the same time
	delay := c.faker.IntRange(0, c.carRunInterval)
	time.Sleep(time.Duration(delay) * time.Millisecond)

	for {
		time.Sleep(time.Duration(c.carRunInterval) * time.Millisecond)
		if c.InUse {
			c.Run()
			log.Logger().Debugf("report current car run status is %v ", c.Event())
			c.channels.CarChannel <- rxgo.Of(c.Event())
		} else {
			c.idleDuration += 1

			// check expiration
			c.ExpireBook()

			if c.idleDuration >= CAR_REPORT_IDLE_DURATION {
				log.Logger().Debugf("report current car idle status is %v ", c.Event())
				c.idleDuration = 0
				c.channels.CarChannel <- rxgo.Of(c.Event())
			}
		}
	}
}

func (c *Car) StopSimulation() {

}

func (c *Car) IsInUse() bool {
	return c.InUse
}

func (c *Car) IsInService() bool {
	return c.inService
}

func (c *Car) IsBooked() bool {
	return c.booked
}

func (c *Car) Book(user *User) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.booked {
		log.Logger().Warnf("car %s is booked by other user %s", c.ID, user.ID)
		return false
	}

	log.Logger().Debugf("car %s is booked by user %s", c.ID, user.ID)
	c.booked = true
	c.currentBooking = NewBooking(user, c)
	user.InTrip = true

	c.channels.BookingChannel <- rxgo.Of(c.currentBooking.Event())
	return true
}

func (c *Car) CancelBook(user *User) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.booked || c.currentBooking == nil {
		return
	}

	log.Logger().Debugf("book %s is cancelled by user %s", c.ID, user.ID)
	user.InTrip = false
	c.booked = false
	c.currentBooking.Act("cancel")
	c.channels.BookingChannel <- rxgo.Of(c.currentBooking.Event())

	c.currentBooking = nil
}

func (c *Car) ExpireBook() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.booked || c.currentBooking == nil {
		return
	}

	if time.Now().UTC().After(c.currentBooking.Expire) {
		log.Logger().Debugf("booking %s is expired", c.currentBooking.ID)
		c.booked = false
		c.currentBooking.Act("expire")
		c.channels.BookingChannel <- rxgo.Of(c.currentBooking.Event())
		c.currentBooking = nil
	}
}

func (c *Car) StartTrip(user *User) {
	c.lock.Lock()
	defer c.lock.Unlock()

	log.Logger().Debugf("user %s attempts to drive car %s", user.ID, c.ID)

	bid := ""
	if !c.IsBooked() || c.currentBooking == nil {
		log.Logger().Warnf("User %s try to start a trip with a car not booked, probably expired", user.ID)
		return
	} else {
		bid = c.currentBooking.ID
	}

	if c.currentBooking.UID == user.ID {
		c.currentBooking.Action = "service"
		c.channels.BookingChannel <- rxgo.Of(c.currentBooking.Event())
	} else {
		log.Logger().Warnf("User %s try to start a trip with a car not booked by him", user.ID)
		return
	}

	user.InTrip = true
	c.InUse = true
	c.Locked = false
	if route, err := NewTrack(c.routes); err != nil {
		log.Logger().Fatalf("invalid route found")
	} else {
		c.route = route
		c.Latitude = c.route.Latitude()
		c.Longitude = c.route.Longitude()
	}

	c.currentTrip = NewTrip(bid, c.Longitude, c.Latitude, c.TotalDistance)
	c.currentUser = user
}

func (c *Car) Run() {
	c.idleDuration = 0
	speed := c.route.Run(float64(c.carRunInterval))
	distance := c.route.Distance()
	c.TotalDistance += distance
	c.Speed = int(speed)
	newLocation := c.route.CurrentLocation()
	c.Latitude = newLocation.Latitude
	c.Longitude = newLocation.Longitude

	c.GasPercent = c.GasPercent - (distance / viper.GetFloat64("cardemo.car.recharge.mileage"))

	if c.GasPercent < REFILL_THRESHOLD {
		c.Refill()
	}

	// arrived!
	if c.route.IsFinished() {
		c.StopTrip()
	}
}

func (c *Car) StopTrip() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.InUse = false
	c.booked = false
	c.Locked = true

	c.currentUser.InTrip = false

	c.currentTrip.End(c.Longitude, c.Latitude, c.TotalDistance)
	c.currentTrip.Pay(c.price, "card") // changed from c.faker.Regex("(balance|card)")
	log.Logger().Debugf("trip end %v", c.currentTrip.Event())

	c.channels.TripChannel <- rxgo.Of(c.currentTrip.Event())
}

func (c *Car) Refill() {
	c.GasPercent = c.faker.Float64Range(REFILL_MIN, REFILL_MAX)
}

func (c *Car) Event() map[string]any {
	var event map[string]any
	err := mapstructure.Decode(c, &event)

	if err != nil {
		log.Logger().Fatal("failed to decode car event")
	}

	event["time"] = time.Now().UTC()
	return event
}

func CreateCar(cid string, inService bool, channels AppChannels, routes *RouteList) *Car {
	log.Logger().Debugf("car: %s in-service: %t", cid, inService)
	car := NewCar(cid, inService, channels, fake.New(0), routes)
	return car
}
