package common

import (
	"fmt"

	fake "github.com/brianvoe/gofakeit/v6"
)

type DimCar struct {
	ID        string `fake:"{c#####}"`
	License   string `fake:"{regex:[A-Z][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]}"`
	InService bool   `fake:"{bool}"`
}

func NewDimCar(id int) *DimCar {
	var car DimCar
	if err := fake.Struct(&car); err != nil {
		// this should never fail
		panic(fmt.Errorf("failed to generate DimCar data due to %w", err))
	}
	car.ID = fmt.Sprintf("c%05d", id)
	car.InService = fake.Float64Range(0.0, 1.0) < 0.9 // 90% precent of car are in service
	return &car
}

func (c *DimCar) ToEvent() map[string]any {
	result := make(map[string]any)
	result["cid"] = c.ID
	result["license_plate_no"] = c.License
	result["in_service"] = c.InService
	return result
}

func (c *DimCar) ToRow() []any {
	return []any{c.ID, c.License, c.InService}
}

func GetDimCarHeader() []string {
	return []string{"cid", "license_plate_no", "in_service"}
}

func MakeDimCars(count int) []*DimCar {
	result := make([]*DimCar, count)
	for i := range result {
		result[i] = NewDimCar(i)
	}
	return result
}
