package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx driver with database/sql
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// Test Client Setup
func testClient(t *testing.T) (*sqlx.DB, Config, *Postgres) {
	t.Helper()

	config := Config{
		Host:             "localhost",
		Port:             5432,
		Username:         "olake",
		Password:         "olake",
		Database:         "testdb",
		SSLConfiguration: &utils.SSLConfig{Mode: "disable"},
		BatchSize:        10000,
		UpdateMethod: &CDC{
			ReplicationSlot: "olake_slot",
			InitialWaitTime: 9,
		},
	}

	d := &Postgres{
		Driver: base.NewBase(),
		config: &config,
	}

	// Properly initialize State
	d.CDCSupport = true
	state := types.NewState(types.GlobalType)
	d.SetupState(state) // Assuming SetupState sets d.State = state

	_ = protocol.ChangeStreamDriver(d) // Only if ChangeStreamDriver is needed
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config, d
}
