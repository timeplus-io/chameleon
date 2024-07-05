package utils

import (
	"fmt"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func GetWithDefault(properties map[string]interface{}, field string, defaultValue string) (string, error) {
	v, exists := properties[field]
	if !exists {
		return defaultValue, nil
	}

	if str, ok := v.(string); ok {
		return str, nil
	}

	return "", fmt.Errorf("value of %s field is not a string", field)
}

func GetIntWithDefault(properties map[string]interface{}, field string, defaultValue int) (int, error) {
	v, exists := properties[field]
	if !exists {
		return defaultValue, nil
	}

	if float, ok := v.(float64); ok {
		return int(float), nil
	}

	if i, ok := v.(int); ok {
		return i, nil
	}

	return 0, fmt.Errorf("value of %s field is not an integer", field)
}

func GetBoolWithDefault(properties map[string]interface{}, field string, defaultValue bool) (bool, error) {
	v, exists := properties[field]
	if !exists {
		return defaultValue, nil
	}

	if b, ok := v.(bool); ok {
		return b, nil
	}

	return false, fmt.Errorf("value of %s field is not a boolean", field)
}
