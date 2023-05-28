package demo

import (
	"encoding/json"
	"io/ioutil"
)

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
