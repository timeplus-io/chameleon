package models

import (
	"strconv"

	"github.com/pkg/errors"
)

type CrimeCase struct {
	Lat                 float32 `db:lat`
	Lon                 float32 `db:lon`
	Description         string  `db:description`
	LocationDescription string  `db:location_description`
	PrimaryType         string  `db:primary_type`
}

func (cc *CrimeCase) Parse(record []string) error {
	if len(record) != 5 {
		return errors.New("expecting 5 elements in record")
	}

	lat, err := strconv.ParseFloat(record[0], 32)
	if err != nil {
		return errors.Wrap(err, "failed to parse lat")
	}

	lon, err := strconv.ParseFloat(record[1], 32)
	if err != nil {
		return errors.Wrap(err, "failed to parse lon")
	}

	cc.Lat = float32(lat)
	cc.Lon = float32(lon)
	cc.Description = record[2]
	cc.LocationDescription = record[3]
	cc.PrimaryType = record[4]

	return nil
}
