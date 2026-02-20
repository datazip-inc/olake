package driver

import (
	"strings"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
)

// resolveMongoDBTimeZone returns a *time.Location for interpreting timestamp values.
// MongoDB stores datetime in UTC by default, but can be configured.
// Priority: connection timezone parameter > UTC default
func resolveMongoDBTimeZone(timezoneParam string) *time.Location {
	// Normalize timezone parameter from connection string
	normalize := func(s string) string {
		return strings.TrimSpace(s)
	}

	tz := normalize(timezoneParam)

	// MongoDB defaults to UTC if no timezone is specified
	if tz == "" {
		logger.Debugf("Using MongoDB default timezone: UTC")
		return time.UTC
	}

	// Try to load the specified timezone
	loc, err := time.LoadLocation(tz)
	if err != nil {
		logger.Warnf("failed to load MongoDB timezone '%s': %s, defaulting to UTC", tz, err)
		return time.UTC
	}

	logger.Debugf("Using MongoDB timezone: %s", tz)
	return loc
}
