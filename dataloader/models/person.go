package models

import (
	"fmt"
	"math/rand"
	"time"

	uuid "github.com/satori/go.uuid"
)

type Person struct {
	Name      string    `db:"name"`
	ID        string    `db:"id"`
	Age       uint8     `db:"age"`
	Sex       string    `db:"sex"`
	Phone     string    `db:"phone"`
	Region    string    `db:"region"`
	City      string    `db:"city"`
	Address   string    `db:"address"`
	Lat       float32   `db:"lat"`
	Lon       float32   `db:"lon"`
	Timestamp time.Time `db:"timestamp"`
}

func generatePerson(ts time.Time, devIndex int, region string, location LatLon) Person {
	rand.Seed(time.Now().UTC().UnixNano())

	remainder := rand.Uint32() % 2
	sex := "male"
	if remainder == 0 {
		sex = "female"
	}
	return Person{
		Name:      fmt.Sprintf("person-%d", devIndex),
		ID:        uuid.NewV4().String(),
		Age:       uint8(rand.Uint32()%120) + 1,
		Sex:       sex,
		Phone:     "13764583893",
		Region:    region,
		City:      location.City,
		Address:   region,
		Lat:       location.Lat,
		Lon:       location.Lon,
		Timestamp: ts,
	}
}

func GeneratePersons(total uint32, locations map[string][]LatLon) []Person {
	ts := time.Now()

	records := make([]Person, 0, total)
	for k := range regionMap {
		for i := 0; i < int(total)/len(regionMap); i++ {
			records = append(records, generatePerson(ts, i, k, locations[k][i]))
		}
	}

	return records
}
