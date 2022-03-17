package models

import (
	"fmt"
	"math/rand"
	"time"
)

type Metric struct {
	Devicename          string    `json:"devicename"`
	Region              string    `json:"region"`
	City                string    `json:"city"`
	Version             string    `json:"version"`
	Lat                 float32   `json:"lat"`
	Lon                 float32   `json:"lon"`
	Battery             float32   `json:"battery"`
	Humidity            uint16    `json:"humidity"`
	Temperature         int16     `json:"temperature"`
	HydraulicPressure   float32   `json:"hydraulic_pressure"`
	AtmosphericPressure float32   `json:"atmospheric_pressure"`
	Timestamp           time.Time `json:"timestamp"`
}

func generateMetric(ts time.Time, devIndex int, region string, location LatLon) Metric {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	return Metric{
		Devicename:          fmt.Sprintf("%s-BHSH-%05d", location.City, devIndex),
		Region:              region,
		City:                location.City,
		Version:             "1.0",
		Lat:                 location.Lat,
		Lon:                 location.Lon,
		Battery:             r.Float32() * 100,
		Humidity:            uint16(r.Uint32()) % uint16(100),
		Temperature:         int16(r.Int31()) % int16(100),
		HydraulicPressure:   1000 + r.Float32()*1000,
		AtmosphericPressure: 101.3 + r.Float32()*100,
		Timestamp:           ts,
	}
}

func GenerateMetrics(totalDevices uint32, locations map[string][]LatLon) []Metric {
	ts := time.Now()

	records := make([]Metric, 0, totalDevices)
	for k := range regionMap {
		for i := 0; i < int(totalDevices)/len(regionMap); i++ {
			records = append(records, generateMetric(ts, i, k, locations[k][i]))
		}
	}

	return records
}
