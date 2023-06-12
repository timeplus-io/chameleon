package demo

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"time"

	"github.com/timeplus-io/chameleon/cardemo/log"
)

const (
	EarthRadius = 6371 // Earth's radius in kilometers
)

type Location struct {
	Latitude  float64
	Longitude float64
}

type Route struct {
	WeightName string   `json:"weight_name"`
	Weight     float64  `json:"weight"`
	Duration   float64  `json:"duration"`
	Distance   float64  `json:"distance"`
	Legs       []Leg    `json:"legs"`
	Geometry   Geometry `json:"geometry"`
}

type Leg struct {
	ViaWaypoints []interface{} `json:"via_waypoints"`
	Admins       []Admin       `json:"admins"`
	Weight       float64       `json:"weight"`
	Duration     float64       `json:"duration"`
	Steps        []interface{} `json:"steps"`
	Distance     float64       `json:"distance"`
	Summary      string        `json:"summary"`
}

type Admin struct {
	Iso31661Alpha3 string `json:"iso_3166_1_alpha3"`
	Iso31661       string `json:"iso_3166_1"`
}

type Geometry struct {
	Coordinates [][]float64 `json:"coordinates"`
	Type        string      `json:"type"`
}

type Waypoint struct {
	Distance float64   `json:"distance"`
	Name     string    `json:"name"`
	Location []float64 `json:"location"`
}

type Response struct {
	Routes    []Route    `json:"routes"`
	Waypoints []Waypoint `json:"waypoints"`
	Code      string     `json:"code"`
	UUID      string     `json:"uuid"`
}

type RouteList []Response

type Track struct {
	position []float64
	path     [][]float64
	step     int

	distance      float64
	totalDistance float64
}

func loadRoutes(path string) (*RouteList, error) {
	jsonData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var routes RouteList
	err = json.Unmarshal([]byte(jsonData), &routes)
	if err != nil {
		return nil, err
	}

	return &routes, nil
}

func NewTrack(routes *RouteList) (*Track, error) {
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(*routes))

	path := (*routes)[randomIndex].Routes[0].Geometry.Coordinates

	if len(path) > 0 {
		return &Track{
			position:      path[0],
			path:          path,
			step:          0,
			distance:      0,
			totalDistance: 0,
		}, nil
	} else {
		return nil, fmt.Errorf("invalid routes")
	}
}

func (t *Track) Latitude() float64 {
	return t.position[1]
}

func (t *Track) Longitude() float64 {
	return t.position[0]
}

func (t *Track) CurrentLocation() Location {
	return Location{
		Latitude:  t.Latitude(),
		Longitude: t.Longitude(),
	}
}

func (t *Track) TargetLocation() Location {
	return Location{
		Latitude:  t.path[t.step+1][1],
		Longitude: t.path[t.step+1][0],
	}
}

func (t *Track) Distance() float64 {
	return t.distance
}

func (t *Track) TotalDistance() float64 {
	return t.totalDistance
}

func (t *Track) Run(interval float64) float64 {
	if t.IsFinished() {
		return 0
	}

	mean := 50.0  // Desired average
	stdDev := 2.0 // Desired standard deviation
	speed := stdDev*rand.NormFloat64() + mean
	t.distance = speed * interval / (60 * 60 * 1000)
	t.totalDistance += t.distance

	log.Logger().Debugf("running distance is %f, interval is %f", t.distance, interval)

	initial := t.CurrentLocation()
	target := t.TargetLocation()

	// Calculate the bearing between the initial and target locations
	bearing := math.Atan2(math.Sin(degToRad(target.Longitude)-degToRad(initial.Longitude))*math.Cos(degToRad(target.Latitude)),
		math.Cos(degToRad(initial.Latitude))*math.Sin(degToRad(target.Latitude))-math.Sin(degToRad(initial.Latitude))*math.Cos(degToRad(target.Latitude))*math.Cos(degToRad(target.Longitude)-degToRad(initial.Longitude)))

	// Convert the bearing to degrees
	bearing = radToDeg(bearing)

	// Calculate the new location after driving 1 kilometer
	newLocation := calculateDestination(initial, bearing, t.distance)

	if (newLocation.Latitude-initial.Latitude) > (target.Latitude-initial.Latitude) ||
		(newLocation.Longitude-initial.Longitude) > (target.Longitude-initial.Longitude) {
		t.step = t.step + 1
		t.position = []float64{target.Longitude, target.Latitude}
	} else {
		t.position = []float64{newLocation.Longitude, newLocation.Latitude}
	}

	return speed
}

func (t *Track) IsFinished() bool {
	if len(t.path) <= 1 {
		return true
	}

	if t.step == len(t.path)-1 {
		return true
	} else {
		return false
	}
}

// Written by ChatGPT
// Converts degrees to radians.
func degToRad(deg float64) float64 {
	return deg * (math.Pi / 180)
}

// Converts radians to degrees.
func radToDeg(rad float64) float64 {
	return rad * (180 / math.Pi)
}

// Calculates the destination location given an initial location, bearing, and distance.
func calculateDestination(initial Location, bearing float64, distance float64) Location {
	lat1 := degToRad(initial.Latitude)
	lon1 := degToRad(initial.Longitude)
	b := degToRad(bearing)
	d := distance / EarthRadius

	lat2 := math.Asin(math.Sin(lat1)*math.Cos(d) + math.Cos(lat1)*math.Sin(d)*math.Cos(b))
	lon2 := lon1 + math.Atan2(math.Sin(b)*math.Sin(d)*math.Cos(lat1), math.Cos(d)-math.Sin(lat1)*math.Sin(lat2))

	return Location{
		Latitude:  radToDeg(lat2),
		Longitude: radToDeg(lon2),
	}
}
