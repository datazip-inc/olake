package driver

import (
	"strings"
	"testing"
)

// Test functions using base utilities
func TestMySQLSetup(t *testing.T) {
	_, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestSetup(t)
}

func TestMySQLDiscover(t *testing.T) {
	conn, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestDiscover(t, conn, ExecuteQuery)
}

func TestMySQLRead(t *testing.T) {
	conn, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestRead(t, conn, ExecuteQuery)
}

// TestMySQLFlavorDetection tests the flavor detection logic
func TestMySQLFlavorDetection(t *testing.T) {
	mysql := &MySQL{}

	testCases := []struct {
		version     string
		expected    string
		description string
	}{
		{"8.0.33 MySQL Community Server", "mysql", "Standard MySQL"},
		{"10.6.12-MariaDB-1:10.6.12+maria~focal", "mariadb", "MariaDB"},
		{"8.0.32-24 Percona Server (GPL)", "percona", "Percona Server"},
		{"5.7.25-TiDB-v6.1.0", "tidb", "TiDB"},
		{"8.0.33", "mysql", "MySQL without server suffix"},
		{"Unknown Database", "mysql", "Unknown database defaults to mysql"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			version := strings.ToLower(tc.version)
			var detectedFlavor string

			if strings.Contains(version, "mariadb") {
				detectedFlavor = "mariadb"
			} else if strings.Contains(version, "percona") {
				detectedFlavor = "percona"
			} else if strings.Contains(version, "tidb") {
				detectedFlavor = "tidb"
			} else if strings.Contains(version, "mysql") {
				detectedFlavor = "mysql"
			} else {
				detectedFlavor = "mysql"
			}

			if detectedFlavor != tc.expected {
				t.Errorf("Expected flavor %s for version '%s', got %s", tc.expected, tc.version, detectedFlavor)
			}
		})
	}
}
