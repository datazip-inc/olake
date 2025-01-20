package utils

import (
	"fmt"
	"path/filepath"
)

// ExtractDirFromPath extracts the directory from a provided file path.
func ExtractDirFromPath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("provided path is empty")
	}
	dir := filepath.Dir(path)
	return dir, nil
}
