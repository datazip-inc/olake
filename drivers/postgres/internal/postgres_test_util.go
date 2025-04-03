package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// Test Client Setup
func testClient(t *testing.T) (*sqlx.DB, Config, *Postgres) {
	t.Helper()

	config := Config{
		Host:             "localhost",
		Port:             5432,
		Username:         "postgres",
		Password:         "secret1234",
		Database:         "postgres",
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
	d.cdcConfig = CDC{
		InitialWaitTime: 5,
		ReplicationSlot: "olake_slot",
	}
	state := types.NewState(types.GlobalType)
	d.SetupState(state) // Assuming SetupState sets d.State = state

	_ = protocol.ChangeStreamDriver(d) // Only if ChangeStreamDriver is needed
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config, d
}
