package demo

import (
	"context"
	"math/rand"
	"time"

	"github.com/timeplus-io/chameleon/cardemo/common"
	"github.com/timeplus-io/chameleon/cardemo/log"
	"github.com/timeplus-io/chameleon/cardemo/sink"

	"github.com/brianvoe/gofakeit/v6"
	fake "github.com/brianvoe/gofakeit/v6"
	"github.com/reactivex/rxgo/v2"
	"github.com/spf13/viper"
)

var TidCounter int
var BidCounter int

const DIMCarTable = "dim_car_info"
const DIMUserTable = "dim_user_info"

func init() {
	TidCounter = 0
	BidCounter = 0
}

type CarSharingDemoApp struct {
	Faker            *fake.Faker
	ScheduleInterval time.Duration

	CarChannel      chan rxgo.Item
	CarStream       rxgo.Observable
	CarStreamCancel rxgo.Disposable

	TripChannel      chan rxgo.Item
	TripStream       rxgo.Observable
	TripStreamCancel rxgo.Disposable

	PaymentChannel      chan rxgo.Item
	PaymentStream       rxgo.Observable
	PaymentStreamCancel rxgo.Disposable

	BookingChannel      chan rxgo.Item
	BookingStream       rxgo.Observable
	BookingStreamCancel rxgo.Disposable

	Routes RouteList
	Users  []*User
	Cars   []*Car

	Stopped bool

	sinks []sink.Sink
}

type AppChannels struct {
	CarChannel     chan rxgo.Item
	TripChannel    chan rxgo.Item
	BookingChannel chan rxgo.Item
}

func initConfig() {
	viper.SetDefault("cardemo.car.update.interval", 10000)  // set default update interval to 10s, the update delta is related
	viper.SetDefault("cardemo.app.schedule.interval", 5000) // set default schedule interval to 5s
	viper.SetDefault("cardemo.user.count", 100)
	viper.SetDefault("cardemo.car.count", 100)
	viper.SetDefault("cardemo.user.batch", 100)
	viper.SetDefault("cardemo.car.batch", 100)
	viper.SetDefault("cardemo.car.update.delta", 0.2/viper.GetFloat64("cardemo.car.update.interval")) // should be ( 0.2 / cardemo.car.update.interval)
	viper.SetDefault("cardemo.car.price", 0.5)                                                        // https://evo.ca/rates per min 0.45, plus $1 for each trip
	viper.SetDefault("cardemo.trip.target.max", 50.0)                                                 // default max km: 50
	viper.SetDefault("cardemo.trip.target.min", 2.0)                                                  // default min km: 2
	viper.SetDefault("cardemo.book.cancel.rate", 0.05)

	viper.SetDefault("cardemo.app.storage.stream.ttl", 2)
	viper.SetDefault("cardemo.app.storage.table.ttl", 24)
	viper.SetDefault("cardemo.app.storage.stream.sizeGiB", 20)

	gofakeit.AddFuncLookup("byear", gofakeit.Info{
		Category:    "custom birthday year",
		Description: "year between 1925 to 2002",
		Example:     "1950",
		Output:      "int",
		Generate: func(r *rand.Rand, m *gofakeit.MapParams, info *gofakeit.Info) (any, error) {
			return gofakeit.IntRange(1925, 2002), nil
		},
	})
}

func NewCarSharingDemoApp() (*CarSharingDemoApp, error) {
	initConfig()
	sceduleInterval := viper.GetInt("cardemo.app.schedule.interval")
	log.Logger().Infof("set car schedule interval to %d ms", sceduleInterval)

	app := CarSharingDemoApp{
		Faker:            fake.New(0),
		Stopped:          false,
		ScheduleInterval: time.Duration(sceduleInterval) * time.Millisecond,
	}

	app.CarChannel = make(chan rxgo.Item)
	app.CarStream = rxgo.FromChannel(app.CarChannel, rxgo.WithPublishStrategy())

	app.TripChannel = make(chan rxgo.Item)
	app.TripStream = rxgo.FromChannel(app.TripChannel)

	app.BookingChannel = make(chan rxgo.Item)
	app.BookingStream = rxgo.FromChannel(app.BookingChannel)

	sinkConfigFile := viper.GetString("sinks-config")
	if sinks, err := sink.LoadSinks(sinkConfigFile); err != nil {
		log.Logger().Errorf("load sinks from %s failed", sinkConfigFile)
		return nil, err
	} else {
		log.Logger().Infof("load sinks from %s success with count %d", sinkConfigFile, len(sinks))
		app.sinks = sinks
		for _, sink := range app.sinks {
			sink.Init()
		}
	}

	go app.handleStreamUpdate(app.CarStream, "car_live_data", "time")
	go app.handleStreamUpdate(app.TripStream, "trips", "end_time")
	go app.handleStreamUpdate(app.BookingStream, "bookings", "time")

	return &app, nil
}

func (a *CarSharingDemoApp) handleStreamUpdate(stream rxgo.Observable, streamName string, timeCol string) {
	disposed := stream.ForEach(func(v any) {
		log.Logger().Debugf("received steam update: %v\n", v)
		event := v.(map[string]any)
		for _, sink := range a.sinks {
			go sink.Send(event, streamName, timeCol)
		}
	}, func(err error) {
		log.Logger().Error("event stream failed", err)
	}, func() {
		log.Logger().Infof("event stream is closed")
	})

	stream.Connect(context.Background())
	<-disposed
}

func (a *CarSharingDemoApp) initRoutes() {
	routesPath := viper.GetString("routes-file")
	if routes, err := loadRoutes(routesPath); err != nil {
		log.Logger().Panicf("failed to load routes from %s", routesPath)
	} else {
		a.Routes = *routes
		log.Logger().Infof("success to load routes from %s", routesPath)
	}
}

func (a *CarSharingDemoApp) initCarsData(channels AppChannels) []*Car {
	carCount := viper.GetInt("cardemo.car.count")
	cars := common.MakeDimCars(carCount)
	for _, sink := range a.sinks {
		sink.InitCars(cars)
	}

	result := make([]*Car, carCount)

	for index, car := range cars {
		result[index] = CreateCar(car.ID, car.InService, channels, &a.Routes)
	}
	return result
}

func (a *CarSharingDemoApp) initUserData() []*User {
	userCount := viper.GetInt("cardemo.user.count")
	users := common.MakeDimUsers(userCount)
	for _, sink := range a.sinks {
		sink.InitUsers(users)
	}
	result := make([]*User, userCount)

	for index, user := range users {
		result[index] = NewUser(user.ID)
	}
	return result
}

func (a *CarSharingDemoApp) start() {
	for {
		if a.Stopped {
			break
		}

		time.Sleep(a.ScheduleInterval)
		user := a.FindUserWantTrip()
		if user != nil {
			car := a.FindAvailableCar()

			if car == nil {
				continue
			}

			booking := a.Faker.Bool() // some user does not book
			log.Logger().Debugf("user decide to book a car %t", booking)
			if booking {
				go func() {
					if !car.Book(user) {
						// failed to book
						return
					}
					// wait time from booking to reach car
					waitTime := a.Faker.IntRange(MIN_BOOK_TO_CAR_TIME, MAX_BOOK_TO_CAR_TIME)

					if a.Faker.Float64Range(0.0, 1.0) < viper.GetFloat64("cardemo.book.cancel.rate") {
						cancelTime := time.Duration(a.Faker.IntRange(0, waitTime))
						time.Sleep(time.Duration(cancelTime) * time.Second)
						car.CancelBook(user)
					} else {
						time.Sleep(time.Duration(waitTime) * time.Second)
						car.StartTrip(user)
					}
				}()
			} else {
				car.Book(user) // book without wait
				car.StartTrip(user)
			}
		}
	}
}

func (a *CarSharingDemoApp) Start() {
	channels := AppChannels{
		CarChannel:     a.CarChannel,
		TripChannel:    a.TripChannel,
		BookingChannel: a.BookingChannel,
	}

	a.initRoutes()
	a.Cars = a.initCarsData(channels)
	a.Users = a.initUserData()

	log.Logger().Infof("initialize demo car/user with number %d %d", len(a.Cars), len(a.Users))
	a.start()
}

func (a *CarSharingDemoApp) FindAvailableCar() *Car {
	if len(a.Cars) == 0 {
		return nil
	}

	for _, car := range a.Cars {
		if !car.IsInUse() && !car.IsBooked() && car.IsInService() {
			return car
		}
	}

	log.Logger().Debug("all cars are in use")
	return nil
}

func (a *CarSharingDemoApp) FindUserWantTrip() *User {
	if len(a.Users) == 0 {
		return nil
	}
	log.Logger().Debugf("searching users not in trip %d", len(a.Users))
	for _, user := range a.Users {
		if !user.InTrip {
			return user
		}
	}

	log.Logger().Debug("all users are in trip")
	return nil
}

func (a *CarSharingDemoApp) Stop() {
}
