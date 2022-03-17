package models

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type UnstructureMetric struct {
	DeviceName string    `db:"devicename"`
	Region     string    `db:"region"`
	City       string    `db:"city"`
	Lat        float32   `db:"lat"`
	Lon        float32   `db:"lon"`
	Raw        string    `db:"raw"`
	Sourcetype string    `db:"sourcetype"`
	IndexTime  time.Time `db:"_index_time"`
}

type UnstructureJsonMetric struct {
	DeviceName string  `json:"devicename"`
	Value      float32 `json:"value"`
	Timestamp  uint32  `json:"timestamp"`
}

func generateHeight(devicename string, ts uint32, indexTime time.Time, region string, latLon LatLon) UnstructureMetric {
	// CSV
	raw := fmt.Sprintf("%s,%f,%d", devicename, randRangeFloat32(0.4, 0.6), ts)
	return UnstructureMetric{
		DeviceName: devicename,
		Region:     region,
		City:       latLon.City,
		Lat:        latLon.Lat,
		Lon:        latLon.Lon,
		Raw:        raw,
		Sourcetype: "height",
		IndexTime:  indexTime,
	}
}

func generatePressure(devicename string, ts uint32, indexTime time.Time, region string, latLon LatLon) UnstructureMetric {
	// Syslog
	raw := fmt.Sprintf("%s %f %d", devicename, randRangeFloat32(0.2, 0.4), ts)

	return UnstructureMetric{
		DeviceName: devicename,
		Region:     region,
		City:       latLon.City,
		Lat:        latLon.Lat,
		Lon:        latLon.Lon,
		Raw:        raw,
		Sourcetype: "pressure",
		IndexTime:  indexTime,
	}
}

func doGenerateConcentration(devicename string, ts uint32, indexTime time.Time, value float32, region string, latLon LatLon) UnstructureMetric {
	// JSON
	m := UnstructureJsonMetric{
		DeviceName: devicename,
		Value:      value,
		Timestamp:  ts,
	}

	data, _ := json.Marshal(&m)

	return UnstructureMetric{
		DeviceName: devicename,
		Region:     region,
		City:       latLon.City,
		Lat:        latLon.Lat,
		Lon:        latLon.Lon,
		Raw:        string(data),
		Sourcetype: "concentration",
		IndexTime:  indexTime,
	}
}

func generateConcentration(source *Source, loc LatLon, region string, devicename string, ts uint32, indexTime time.Time, deadline uint32, lastRun map[string]float32) UnstructureMetric {
	k := fmt.Sprintf("%s:%s:%s", region, loc.City, devicename)

	now := uint32(time.Now().Unix())
	if now < deadline {
		value := randRangeFloat32(0.2, 0.3) * source.Settings.InitialBase
		lastRun[k] = value
		return doGenerateConcentration(devicename, ts, indexTime, value, region, loc)
	}

	last := lastRun[k]
	// increasing until hit max
	last += randRangeFloat32(0.5, 0.7)*source.Settings.Step + source.Settings.Step

	if last > source.Settings.MaxValue {
		last = source.Settings.MaxValue - rand.Float32()
	}

	lastRun[k] = last
	return doGenerateConcentration(devicename, ts, indexTime, last, region, loc)
}

func GenerateUnstructureMetrics(source *Source, devLocations map[string][]LatLon, deadline uint32, lastRun map[string]float32) []UnstructureMetric {
	records := make([]UnstructureMetric, 0, source.Settings.TotalEntities)
	ts := time.Now()

	for region := range regionMap {
		for i := 0; i < int(source.Settings.TotalEntities)/len(regionMap); i++ {
			var record UnstructureMetric
			deviceName := fmt.Sprintf("%s-BHSH-%05d", devLocations[region][i].City, i+1)

			if source.Settings.Sourcetype == "pressure" {
				record = generatePressure(deviceName, uint32(ts.Unix()), ts, region, devLocations[region][i])
			} else if source.Settings.Sourcetype == "height" {
				record = generateHeight(deviceName, uint32(ts.Unix()), ts, region, devLocations[region][i])
			} else {
				record = generateConcentration(source, devLocations[region][i], region, deviceName, uint32(ts.Unix()), ts, deadline, lastRun)
			}
			records = append(records, record)
		}
	}
	return records
}
