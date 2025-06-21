package driver

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const (
	defaultOracleUser     = "system"
	defaultOracleHost     = "localhost"
	defaultOraclePort     = 1521
	defaultOraclePassword = "oracle"
	defaultOracleDatabase = "XE"
	defaultMaxThreads     = 4
	defaultRetryCount     = 3
	initialCDCWaitTime    = 5
)

// ExecuteQuery executes Oracle queries for testing based on the operation type
func ExecuteQuery(ctx context.Context, t *testing.T, conn interface{}, tableName string, operation string) {
	t.Helper()

	db, ok := conn.(*sqlx.DB)
	require.True(t, ok, "Expected *sqlx.DB connection")

	var (
		query string
		err   error
	)

	switch operation {
	case "create":
		query = fmt.Sprintf(`
			CREATE TABLE %s (
				id NUMBER PRIMARY KEY,
				col1 VARCHAR2(255),
				col2 VARCHAR2(255)
			)`, tableName)

	case "drop":
		query = fmt.Sprintf("DROP TABLE %s", tableName)

	case "clean":
		query = fmt.Sprintf("DELETE FROM %s", tableName)

	case "add":
		insertTestData(t, ctx, db, tableName)
		return // Early return since we handle all inserts in the helper function

	case "insert":
		query = fmt.Sprintf(`
			INSERT INTO %s (id, col1, col2) 
			VALUES (10, 'new val', 'new val')`, tableName)

	case "update":
		query = fmt.Sprintf(`
			UPDATE %s 
			SET col1 = 'updated val' 
			WHERE id = (
				SELECT id FROM (
					SELECT id FROM %s ORDER BY DBMS_RANDOM.VALUE() FETCH FIRST 1 ROW ONLY
				)
			)`, tableName, tableName)

	case "delete":
		query = fmt.Sprintf(`
			DELETE FROM %s 
			WHERE id = (
				SELECT id FROM (
					SELECT id FROM %s ORDER BY DBMS_RANDOM.VALUE() FETCH FIRST 1 ROW ONLY
				)
			)`, tableName, tableName)

	default:
		t.Fatalf("Unsupported operation: %s", operation)
	}

	_, err = db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to execute %s operation", operation)
}

// insertTestData inserts test data into the specified table
func insertTestData(t *testing.T, ctx context.Context, db *sqlx.DB, tableName string) {
	t.Helper()

	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf(`
			INSERT INTO %s (id, col1, col2) 
			VALUES (%d, 'value%d_col1', 'value%d_col2')`,
			tableName, i, i, i)

		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
}

// testAndBuildAbstractDriver initializes and returns an AbstractDriver with default configuration
func testAndBuildAbstractDriver(t *testing.T) (*sqlx.DB, *abstract.AbstractDriver) {
	t.Helper()
	logger.Init()

	config := Config{
		Username:   defaultOracleUser,
		Host:       defaultOracleHost,
		Port:       defaultOraclePort,
		Password:   defaultOraclePassword,
		Database:   defaultOracleDatabase,
		ServiceName: "XE", // Oracle Express Edition default service
		MaxThreads: defaultMaxThreads,
		RetryCount: defaultRetryCount,
	}

	oracleDriver := &Oracle{
		config: &config,
		cdcConfig: CDC{
			InitialWaitTime: initialCDCWaitTime,
		},
	}
	oracleDriver.CDCSupport = true
	absDriver := abstract.NewAbstractDriver(context.Background(), oracleDriver)

	state := &types.State{
		Type:    types.StreamType,
		RWMutex: &sync.RWMutex{},
	}
	absDriver.SetupState(state)
	require.NoError(t, absDriver.Setup(context.Background()), "Failed to setup Oracle driver")

	return oracleDriver.client, absDriver
} 