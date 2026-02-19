package driver

import (
	"strings"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
)

// resolvePostgreSQLTimeZone returns a *time.Location for interpreting timestamp values.
// PostgreSQL stores timezone in session or database level.
// Priority: session timezone > database timezone > UTC default
func resolvePostgreSQLTimeZone(sessionTimezone, dbTimezone string) *time.Location {
	normalize := func(s string) string {
		return strings.TrimSpace(s)
	}

	session := normalize(sessionTimezone)
	db := normalize(dbTimezone)

	var name string
	switch {
	case session != "":
		name = session
	case db != "":
		name = db
	default:
		name = "UTC"
	}

	loc, err := time.LoadLocation(name)
	if err != nil {
		logger.Warnf("failed to load PostgreSQL timezone '%s': %s, defaulting to UTC", name, err)
		return time.UTC
	}

	logger.Debugf("Using PostgreSQL timezone: %s", name)
	return loc
}
