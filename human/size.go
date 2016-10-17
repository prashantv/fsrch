package human

import (
	"fmt"
	"strconv"
	"strings"
)

// SizeUnit represents the different human units to display.
type SizeUnit int64

// The different size units that we show human suffixes for.
const (
	Byte     SizeUnit = 1
	Kilobyte SizeUnit = 1024
	Megabyte SizeUnit = 1024 * Kilobyte
	Gigabyte SizeUnit = 1024 * Megabyte
	Terabyte SizeUnit = 1024 * Gigabyte
	Petabyte SizeUnit = 1024 * Terabyte
)

func (s SizeUnit) String() string {
	switch s {
	case Byte:
		return "B"
	case Kilobyte:
		return "K"
	case Megabyte:
		return "M"
	case Gigabyte:
		return "G"
	case Terabyte:
		return "T"
	case Petabyte:
		return "P"
	}
	panic(fmt.Sprintf("unknown size unit: %d", s))
}

// Format returns the size formatted with the specified unit.
func (s SizeUnit) Format(bs int64) string {
	return fmt.Sprintf("%s%v", formatFloat(float64(bs)/float64(s)), s)
}

func formatFloat(f float64) string {
	return strings.TrimSuffix(strconv.FormatFloat(f, 'f', 1, 64), ".0")
}

func largestUnit(bs int64) SizeUnit {
	switch {
	case bs > int64(Petabyte):
		return Petabyte
	case bs > int64(Terabyte):
		return Terabyte
	case bs > int64(Gigabyte):
		return Gigabyte
	case bs > int64(Megabyte):
		return Megabyte
	case bs > int64(Kilobyte):
		return Kilobyte
	default:
		return Byte
	}
}

// Size returns a human-readable formatted version of the size.
func Size(bs int64) string {
	return largestUnit(bs).Format(bs)
}
