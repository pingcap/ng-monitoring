package util

import (
	"time"
)

func GetTimeStamp(t time.Time) int64 {
	return t.Unix()
}
