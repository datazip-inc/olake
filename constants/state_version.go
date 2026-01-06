package constants

// State version constants for backward compatibility
// State files can have different versions to support migration and backward compatibility
// when the state file format or behavior changes.

// LatestStateVersion is the current version of the state file format.
// This version is used when creating new state files.
//
// Version History:
//   - Version 0: Legacy format (backward compatibility)
//     * More lenient date/timestamp parsing behavior
//     * When a string cannot be parsed as a timestamp, it returns epoch time (1970-01-01)
//     * Used for state files created before version 1 was introduced
//
//   - Version 1: Current format (introduced stricter validation)
//     * Stricter date/timestamp parsing validation
//     * When a string cannot be parsed as a timestamp (and isTimestampInDB is false), it will be returned as string
//     * This prevents data corruption by failing fast on invalid date strings

const (
	LatestStateVersion = 1
)

// Used as the current version of the state when the program is running
var LoadedStateVersion = 1
