package common

import (
	"math"
	"time"
)

func GetUnixMilliseconds(time time.Time) int64 {
	return time.UnixNano() / 1000000
}

func FromUnixMilliseconds(milliseconds int64) time.Time {
	return FromUnixNanoseconds(MillisecondsToNanoseconds(milliseconds))
}

func FromUnixNanoseconds(nanoseconds int64) time.Time {
	return time.Unix(0, nanoseconds)
}

func MillisecondsToNanoseconds(milliseconds int64) int64 {
	return milliseconds * int64(time.Millisecond/time.Nanosecond)
}

func MillisecondsFloatToNanoseconds(milliseconds float64) int64 {
	return int64(math.Round(milliseconds * float64(time.Millisecond/time.Nanosecond)))
}
