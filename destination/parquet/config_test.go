package parquet

import "testing"

func TestConfigValidateDefaults(t *testing.T) {
	config := &Config{}

	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	if config.MaxFileSizeMB != DefaultMaxFileSizeMB {
		t.Errorf("Expected MaxFileSizeMB to be %d, got %d", DefaultMaxFileSizeMB, config.MaxFileSizeMB)
	}

	if config.MaxRowsPerFile != DefaultMaxRowsPerFile {
		t.Errorf("Expected MaxRowsPerFile to be %d, got %d", DefaultMaxRowsPerFile, config.MaxRowsPerFile)
	}
}

func TestConfigValidateCustomValues(t *testing.T) {
	config := &Config{
		MaxFileSizeMB:  256,
		MaxRowsPerFile: 500000,
	}

	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	if config.MaxFileSizeMB != 256 {
		t.Errorf("Expected MaxFileSizeMB to be 256, got %d", config.MaxFileSizeMB)
	}

	if config.MaxRowsPerFile != 500000 {
		t.Errorf("Expected MaxRowsPerFile to be 500000, got %d", config.MaxRowsPerFile)
	}
}
