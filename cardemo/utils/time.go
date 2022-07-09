package utils

import (
	"strconv"
	"time"
)

func GetTimestamp() string {
	now := time.Now()
	nsec := now.UnixNano()
	return strconv.FormatInt(nsec, 10)
}
