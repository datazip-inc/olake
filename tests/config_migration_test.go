package tests

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

// oldConfig represents the old configuration format with default_mode at the top level
type oldConfig struct {
	Host         string         `json:"host"`
	Port         int            `json:"port"`
	Database     string         `json:"database"`
	Username     string         `json:"username"`
	Password     string         `json:"password"`
	DefaultMode  types.SyncMode `json:"default_mode"`
}

// newConfig represents the new configuration format with sync_settings
type newConfig struct {
	Host         string         `json:"host"`
	Port         int            `json:"port"`
	Database     string         `json:"database"`
	Username     string         `json:"username"`
	Password     string         `json:"password"`
	SyncSettings *syncSettings  `json:"sync_settings"`
}

type syncSettings struct {
	Mode types.SyncMode `json:"mode"`
}

// TestConfigMigration verifies our changes to the configuration format
func TestConfigMigration(t *testing.T) {
	// Test case 1: Old format with default_mode
	oldFormatStr := `{
		"host": "localhost",
		"port": 5432,
		"database": "testdb",
		"username": "user",
		"password": "pass",
		"default_mode": "full_refresh"
	}`

	// Test case 2: New format with sync_settings
	newFormatStr := `{
		"host": "localhost",
		"port": 5432,
		"database": "testdb",
		"username": "user",
		"password": "pass",
		"sync_settings": {
			"mode": "full_refresh"
		}
	}`

	// Test case 3: Catalog with default_mode
	catalogStr := `{
		"streams": [
			{
				"stream": {
					"name": "test_table",
					"namespace": "public"
				}
			}
		],
		"default_mode": "full_refresh"
	}`

	// Create temporary directory for test files
	tempDir, err := ioutil.TempDir("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Write test files
	oldConfigPath := filepath.Join(tempDir, "old_config.json")
	newConfigPath := filepath.Join(tempDir, "new_config.json")
	catalogPath := filepath.Join(tempDir, "catalog.json")

	if err := ioutil.WriteFile(oldConfigPath, []byte(oldFormatStr), 0644); err != nil {
		t.Fatalf("Failed to write old config file: %v", err)
	}
	if err := ioutil.WriteFile(newConfigPath, []byte(newFormatStr), 0644); err != nil {
		t.Fatalf("Failed to write new config file: %v", err)
	}
	if err := ioutil.WriteFile(catalogPath, []byte(catalogStr), 0644); err != nil {
		t.Fatalf("Failed to write catalog file: %v", err)
	}

	// Test old config format
	var oldCfg oldConfig
	oldData, _ := ioutil.ReadFile(oldConfigPath)
	if err := json.Unmarshal(oldData, &oldCfg); err != nil {
		t.Fatalf("Failed to unmarshal old config: %v", err)
	}
	assert.Equal(t, types.FULLREFRESH, oldCfg.DefaultMode, "DefaultMode should be properly parsed from old format")

	// Test new config format
	var newCfg newConfig
	newData, _ := ioutil.ReadFile(newConfigPath)
	if err := json.Unmarshal(newData, &newCfg); err != nil {
		t.Fatalf("Failed to unmarshal new config: %v", err)
	}
	assert.Equal(t, types.FULLREFRESH, newCfg.SyncSettings.Mode, "Mode should be properly parsed from sync_settings")

	// Test catalog with default_mode
	var catalog types.Catalog
	catalogData, _ := ioutil.ReadFile(catalogPath)
	if err := json.Unmarshal(catalogData, &catalog); err != nil {
		t.Fatalf("Failed to unmarshal catalog: %v", err)
	}
	assert.Equal(t, types.FULLREFRESH, catalog.DefaultMode, "DefaultMode should be properly parsed from catalog")

	t.Log("All configuration migration tests passed")
} 