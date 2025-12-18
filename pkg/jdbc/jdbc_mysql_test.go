package jdbc

import (
	"strings"
	"testing"
)

func TestMySQLFlavorDetection(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		expected string
	}{
		{
			name:     "MySQL 5.7",
			version:  "5.7.32-0ubuntu0.18.04.1",
			expected: "mysql",
		},
		{
			name:     "MySQL 8.0",
			version:  "8.0.23",
			expected: "mysql",
		},
		{
			name:     "MySQL 8.4",
			version:  "8.4.0",
			expected: "mysql",
		},
		{
			name:     "MariaDB 10.5",
			version:  "10.5.8-MariaDB-1:10.5.8+maria~focal",
			expected: "mariadb",
		},
		{
			name:     "MariaDB 11.0",
			version:  "11.0.1-MariaDB",
			expected: "mariadb",
		},
		{
			name:     "Percona 8.0",
			version:  "8.0.23-14.1-percona-server-community",
			expected: "mysql",
		},
		{
			name:     "Percona 5.7",
			version:  "5.7.32-35-percona-server-community",
			expected: "mysql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flavor := extractFlavorFromVersion(tt.version)
			if flavor != tt.expected {
				t.Errorf("expected %s, got %s for version %s", tt.expected, flavor, tt.version)
			}
		})
	}
}

func extractFlavorFromVersion(version string) string {
	upper := strings.ToUpper(version)
	flavor := "mysql"
	if strings.Contains(upper, "MARIADB") {
		flavor = "mariadb"
	}
	return flavor
}
