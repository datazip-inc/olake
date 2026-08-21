package constants

import (
	_ "embed"
	"encoding/json"
)

// State version constants for backward compatibility
// State files can have different versions to support migration and backward compatibility
// when the state file format or behavior changes.

// Version History:
//   - Version 0: Legacy format (backward compatibility)
//     * More lenient date/timestamp parsing behavior
//     * When a string cannot be parsed as a timestamp, it returns epoch time (1970-01-01)
//     * Used for state files created before version 1 was introduced
//
//   - Version 1: Introduced stricter validation
//     * Stricter date/timestamp parsing validation
//     * When a string cannot be parsed as a timestamp, it will be returned as string. Earlier it was returning epoch time (1970-01-01)
//     * This prevents data corruption by failing fast on invalid date strings
//
//   - Version 2: Introduces consistent timezone handling between MySQL Full Refresh and CDC.
//     * Binlog CDC now uses TimestampStringLocation to align with the connection's timezone configuration.
//     * This prevents discrepancies where CDC timestamps could differ from Full Refresh data.
//
//   - Version 3: Parses the timezone offset for MySQL correctly
//     * Earlier if the session timezone or global was set in offset format, it was not parsed correctly and used to fallback to UTC.
//     * Now it parses the offset correctly and uses the timezone offset to set the timezone for the connection.
//
//   - Version 4: Unsigned int/integer/bigint map to Int64.
//     * Earlier unsigned int/integer/bigint were mapped to Int32 which caused integer overflows.
//
//   - Version 5: MongoDB nested DateTime values decoded as UTC time.Time.
//     * BSON DateTime at any depth is now decoded directly to time.Time (UTC) via a custom client registry, preventing json.Marshal crashes for out-of-range years ([0,9999]).
//     * Top-level DateTime fields that previously formatted with the local machine timezone (e.g. "+05:30") now always output UTC ("Z").
//
//   - Version 6: Added []uint8 (byte slice) support in ReformatInt64
//     * Previously, numeric values returned as byte slices (common in some SQL drivers) caused errors
//     * Now these byte slices are parsed and converted into int64
//
//   - Version 7: (Current Version) Parquet INT96 and unsigned 32-bit columns map to their correct types.
//     * INT96: earlier the raw 96-bit integer was emitted as a string, which disagreed with the inferred Timestamp schema and collapsed the column to String.
//     * Unsigned 32-bit: earlier read as a signed int32 and mapped to Int32, so values above 2^31-1 wrapped negative. Now widened to Int64, matching pg/mysql.
//     * Older state keeps both previous behaviors so existing destination columns do not change type on upgrade.

// LatestStateVersion is the current version of the state file format.
// This version is used when creating new state files.
var LatestStateVersion int

// Used as the current version of the state when the program is running
var LoadedStateVersion int

//go:embed state-versions.json
var rawStateVersions []byte

// init initializes static information only: the version this build writes. The version a running
// sync is pinned at comes from its state file, via SetLoadedStateVersion.
func init() {
	var doc struct {
		LatestStateVersion int `json:"latest_state_version"`
	}
	if err := json.Unmarshal(rawStateVersions, &doc); err != nil {
		panic("constants/state-versions.json is not valid JSON: " + err.Error())
	}
	if doc.LatestStateVersion <= 0 {
		panic("constants/state-versions.json must set latest_state_version to a positive integer")
	}
	LatestStateVersion = doc.LatestStateVersion
	LoadedStateVersion = LatestStateVersion
}
