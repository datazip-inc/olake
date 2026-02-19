package driver

import (
	"strings"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
)

// resolveKafkaTimeZone returns a *time.Location for interpreting timestamp values.
// Kafka timestamps are typically in UTC by default at the broker level.
// Priority: consumer timezone parameter > UTC default
func resolveKafkaTimeZone(timezoneParam string) *time.Location {
	// Normalize timezone parameter from config
	normalize := func(s string) string {
		return strings.TrimSpace(s)
	}

	tz := normalize(timezoneParam)

	// Kafka defaults to UTC if no timezone is specified
	if tz == "" {
		logger.Debugf("Using Kafka default timezone: UTC")
		return time.UTC
	}

	// Try to load the specified timezone
	loc, err := time.LoadLocation(tz)
	if err != nil {
		logger.Warnf("failed to load Kafka timezone '%s': %s, defaulting to UTC", tz, err)
		return time.UTC
	}

	logger.Debugf("Using Kafka timezone: %s", tz)
	return loc
}
