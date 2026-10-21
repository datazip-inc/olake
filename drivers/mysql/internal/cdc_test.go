package driver

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestBinlogFlavor(t *testing.T) {
	tests := []struct {
		name         string
		serverFlavor string
		expected     string
	}{
		{name: "MariaDB server uses the mariadb replication flavor", serverFlavor: "MariaDB", expected: mysql.MariaDBFlavor},
		{name: "MySQL server keeps the mysql replication flavor", serverFlavor: "MySQL", expected: mysql.MySQLFlavor},
		{name: "unrecognized flavor falls back to mysql", serverFlavor: "", expected: mysql.MySQLFlavor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := binlogFlavor(tt.serverFlavor); got != tt.expected {
				t.Errorf("binlogFlavor(%q) = %q, want %q", tt.serverFlavor, got, tt.expected)
			}
		})
	}
}
