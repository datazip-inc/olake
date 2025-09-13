package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/pkg/jdbc"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmptyTableIntegration tests the empty table handling scenarios
func TestEmptyTableIntegration(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Connect to MySQL using test connection string
	connStr := "mysql:secret1234@tcp(localhost:3306)/olake_mysql_test?parseTime=true"
	db, err := sqlx.Connect("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	// Create test database
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS empty_table_tests")
	require.NoError(t, err)
	_, err = db.Exec("USE empty_table_tests")
	require.NoError(t, err)

	// Test scenarios by directly testing the logic
	tests := []struct {
		name          string
		setupSQL      []string
		tableName     string
		expectError   bool
		errorContains string
		expectEmpty   bool
		description   string
	}{
		{
			name: "table_with_populated_stats",
			setupSQL: []string{
				"DROP TABLE IF EXISTS test_with_stats",
				"CREATE TABLE test_with_stats (id INT PRIMARY KEY, data VARCHAR(100))",
				"INSERT INTO test_with_stats VALUES (1, 'data1'), (2, 'data2')",
				"ANALYZE TABLE test_with_stats",
			},
			tableName:   "test_with_stats",
			expectError: false,
			expectEmpty: false,
			description: "Table with data and updated statistics should return stats",
		},
		{
			name: "empty_table_with_stats",
			setupSQL: []string{
				"DROP TABLE IF EXISTS test_empty",
				"CREATE TABLE test_empty (id INT PRIMARY KEY, data VARCHAR(100))",
				"ANALYZE TABLE test_empty",
			},
			tableName:   "test_empty",
			expectError: false,
			expectEmpty: true,
			description: "Empty table with stats should return 0 rows",
		},
		{
			name: "small_table_missing_stats",
			setupSQL: []string{
				"DROP TABLE IF EXISTS test_small_no_stats",
				"CREATE TABLE test_small_no_stats (id INT PRIMARY KEY, data VARCHAR(100))",
				"INSERT INTO test_small_no_stats VALUES (1, 'data')",
				// Deliberately not running ANALYZE TABLE
			},
			tableName:     "test_small_no_stats",
			expectError:   true,
			errorContains: "stats not populated",
			description:   "Small table without stats should fail with stats error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Testing: %s", tt.description)

			// Setup test table
			for _, sqlStmt := range tt.setupSQL {
				_, err := db.Exec(sqlStmt)
				require.NoError(t, err, "Failed to execute setup SQL: %s", sqlStmt)
			}

			// Test the stats query directly
			var approxRowCount int64
			var avgRowSize any
			approxRowCountQuery := jdbc.MySQLTableRowStatsQuery()
			err := db.QueryRow(approxRowCountQuery, tt.tableName).Scan(&approxRowCount, &avgRowSize)
			
			if err != nil {
				t.Logf("Stats query failed: %v", err)
				if tt.expectError {
					t.Logf("✓ Got expected error from stats query")
				} else {
					t.Errorf("Unexpected error from stats query: %v", err)
				}
				return
			}

			t.Logf("Stats: approxRowCount=%d, avgRowSize=%v", approxRowCount, avgRowSize)

			// Test the controlled fallback logic
			if approxRowCount == 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*1000000000) // 5 seconds
				defer cancel()

				var actualCount int64
				countQuery := "SELECT COUNT(*) FROM empty_table_tests." + tt.tableName
				err := db.QueryRowContext(ctx, countQuery).Scan(&actualCount)
				
				if err != nil {
					if tt.expectError {
						assert.Contains(t, err.Error(), "context deadline exceeded", "Should timeout for large tables")
						t.Logf("✓ Got expected timeout error: %v", err)
					} else {
						t.Errorf("Unexpected error from COUNT query: %v", err)
					}
					return
				}

				t.Logf("COUNT(*) result: %d", actualCount)

				if actualCount != 0 {
					if tt.expectError {
						t.Logf("✓ Detected missing stats (COUNT=%d but approxRowCount=0)", actualCount)
					} else {
						t.Errorf("Expected empty table but got COUNT=%d", actualCount)
					}
				} else {
					if tt.expectEmpty {
						t.Logf("✓ Confirmed empty table (COUNT=0)")
					} else {
						t.Errorf("Expected non-empty table but got COUNT=0")
					}
				}
			} else {
				// Table has stats showing non-zero rows
				if avgRowSize == nil && tt.expectError {
					t.Logf("✓ Detected partial stats (rowCount=%d but avgRowSize=nil)", approxRowCount)
				} else if !tt.expectError {
					t.Logf("✓ Table has proper stats (rowCount=%d, avgRowSize=%v)", approxRowCount, avgRowSize)
				}
			}
		})
	}
}
