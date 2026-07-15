// Package logger holds the shared file-writing helper used by both the olake product (which
// re-exports it) and the tests tree.
package logger

import (
	"encoding/json"
	"fmt"
	"os"
)

// FileLoggerWithPath marshals content to JSON and writes it to path (truncating).
func FileLoggerWithPath(content any, path string) error {
	if path == "" {
		return fmt.Errorf("path is not set")
	}
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %s", err)
	}
	if err := os.WriteFile(path, contentBytes, 0644); err != nil {
		return fmt.Errorf("failed to write data to file: %s", err)
	}
	return nil
}
