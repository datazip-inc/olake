package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var utcOffsetRegex = regexp.MustCompile(`^([+-])(0?\d|1\d|2[0-3]):([0-5]\d)$`)

// NormalizeTimeZoneName trims whitespace and surrounding quotes from a timezone value.
func NormalizeTimeZoneName(name string) string {
	return strings.Trim(strings.TrimSpace(name), `'"`)
}

// LoadLocationOrFixedOffset resolves either an IANA timezone name or a fixed UTC offset.
func LoadLocationOrFixedOffset(name string) (*time.Location, error) {
	name = NormalizeTimeZoneName(name)
	if name == "" {
		return nil, fmt.Errorf("timezone is empty")
	}

	if matches := utcOffsetRegex.FindStringSubmatch(name); matches != nil {
		signStr, hourStr, minuteStr := matches[1], matches[2], matches[3]
		hours, err := strconv.Atoi(hourStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone hour %q: %w", hourStr, err)
		}
		minutes, err := strconv.Atoi(minuteStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone minute %q: %w", minuteStr, err)
		}

		offsetSeconds := hours*3600 + minutes*60
		if signStr == "-" {
			offsetSeconds = -offsetSeconds
		}
		return time.FixedZone(name, offsetSeconds), nil
	}

	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	return loc, nil
}
