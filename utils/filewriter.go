package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

// CreateFile creates a new file or overwrites an existing one with the specified filename, path, extension,
func CreateFile(filePath, fileName, fileExtension string, data []byte) error {
	// Construct the full file path
	fullPath := filepath.Join(filePath, fileName+fileExtension)

	// Create or truncate the file
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create or open file: %w", err)
	}
	defer file.Close()

	// Write data to the file
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}

	return nil
}
