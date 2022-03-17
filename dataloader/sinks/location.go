package sinks

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"

	"github.com/timeplus-io/chameleon/dataloader/models"
	"gitlab.com/chenziliang/pkg-go/utils"
)

func GenerateLocations(source *models.Source, centerized bool) map[string][]models.LatLon {
	dbFile := source.Settings.LastRunStateDB
	if utils.FileExists(dbFile) {
		if locations, err := loadLocations(dbFile); err == nil {
			return locations
		}
	}

	locations := models.GenerateLocations(source.Settings.TotalEntities, centerized)

	dumpLocations(locations, dbFile)

	return locations
}

func dumpLocations(locations map[string][]models.LatLon, dbFile string) error {
	// save the location information for next use
	data, err := json.Marshal(&locations)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		return err
	}
	zw.Close()

	return ioutil.WriteFile(dbFile, buf.Bytes(), 0644)
}

func loadLocations(dbFile string) (map[string][]models.LatLon, error) {
	data, err := ioutil.ReadFile(dbFile)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.Write(data)

	zr, err := gzip.NewReader(&buf)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	data, err = ioutil.ReadAll(zr)
	if err != nil {
		return nil, err
	}

	var locations map[string][]models.LatLon
	err = json.Unmarshal(data, &locations)
	return locations, err
}
