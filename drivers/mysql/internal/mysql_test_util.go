// testMySQLClient
package driver

import (
	"database/sql"
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func testMySQLClient(t *testing.T) (*sql.DB, Config, *MySQL) {
	t.Helper()
	config := Config{
		Username:   "mysql",
		Host:       "localhost",
		Port:       3306,
		Password:   "secret1234",
		Database:   "mysql",
		MaxThreads: 4,
		RetryCount: 3,
	}

	// Create MySQL driver instance
	d := &MySQL{
		Driver: base.NewBase(),
		config: &config,
	}

	d.CDCSupport = true
	d.cdcConfig = CDC{
		InitialWaitTime: 5,
	}

	// Set up state
	// Properly initialize State
	d.CDCSupport = true
	state := types.NewState(types.GlobalType)
	d.SetupState(state) // Assuming SetupState sets d.State = state

	_ = protocol.ChangeStreamDriver(d) // Only if ChangeStreamDriver is needed
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config, d
}
