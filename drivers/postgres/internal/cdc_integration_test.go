package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSlot        = "olake_test_slot"
	testPublication = "olake_test_pub"
	testTable       = "public.cdc_test"
	testPort        = 15432
)

// startEmbeddedPostgres starts an embedded PostgreSQL instance with logical replication enabled.
func startEmbeddedPostgres(t *testing.T) *embeddedpostgres.EmbeddedPostgres {
	t.Helper()

	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V16).
			Port(testPort).
			StartParameters(map[string]string{
				"wal_level":             "logical",
				"max_replication_slots": "4",
				"max_wal_senders":       "4",
			}),
	)

	require.NoError(t, pg.Start(), "failed to start embedded postgres")
	return pg
}

// connectDB opens a connection using lib/pq (avoids pgx protocol issues with pg_create_logical_replication_slot).
func connectDB(t *testing.T) *sqlx.DB {
	t.Helper()

	connStr := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", testPort)
	db, err := sqlx.Connect("postgres", connStr)
	require.NoError(t, err)
	return db
}

// setupCDCInfra creates the test table, publication, and replication slot.
func setupCDCInfra(t *testing.T, db *sqlx.DB) {
	t.Helper()

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, val TEXT)", testTable))
	require.NoError(t, err, "failed to create table")

	_, err = db.Exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", testPublication, testTable))
	require.NoError(t, err, "failed to create publication")

	var slotName, consistentPoint string
	err = db.QueryRow(fmt.Sprintf("SELECT * FROM pg_create_logical_replication_slot('%s', 'pgoutput')", testSlot)).
		Scan(&slotName, &consistentPoint)
	require.NoError(t, err, "failed to create replication slot")
}

// newTestState creates a State backed by a temp file so LogState() works.
func newTestState(t *testing.T) *types.State {
	t.Helper()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	viper.Set(constants.StatePath, stateFile)

	return &types.State{
		Type:    types.GlobalType,
		RWMutex: &sync.RWMutex{},
	}
}

// newPostgresDriver creates a minimal Postgres driver instance for CDC testing.
func newPostgresDriver(t *testing.T, db *sqlx.DB, state *types.State) *Postgres {
	t.Helper()

	return &Postgres{
		client:     db,
		CDCSupport: true,
		cdcConfig: CDC{
			ReplicationSlot: testSlot,
			InitialWaitTime: 120,
			Publication:     testPublication,
		},
		state: state,
	}
}

// getSlotLSN returns the current confirmed_flush_lsn for the test slot.
func getSlotLSN(t *testing.T, ctx context.Context, db *sqlx.DB) pglogrepl.LSN {
	t.Helper()

	slot, err := waljs.GetSlotPosition(ctx, db, testSlot)
	require.NoError(t, err)
	return slot.LSN
}

// getStateLSN extracts the LSN stored in OLake's global state.
func getStateLSN(t *testing.T, state *types.State) pglogrepl.LSN {
	t.Helper()

	global := state.GetGlobal()
	require.NotNil(t, global, "global state should not be nil")
	require.NotNil(t, global.State, "global state value should not be nil")

	var ws waljs.WALState
	require.NoError(t, utils.Unmarshal(global.State, &ws))
	require.NotEmpty(t, ws.LSN, "state LSN should not be empty")

	parsed, err := pglogrepl.ParseLSN(ws.LSN)
	require.NoError(t, err)
	return parsed
}

func TestPreCDC_LSNConsistency(t *testing.T) {
	pg := startEmbeddedPostgres(t)
	defer func() { require.NoError(t, pg.Stop()) }()

	db := connectDB(t)
	defer db.Close()

	setupCDCInfra(t, db)

	ctx := context.Background()
	state := newTestState(t)
	driver := newPostgresDriver(t, db, state)

	// PreCDC on a fresh state (no global state yet) should:
	// 1. Advance the replication slot
	// 2. Persist state with the same LSN
	err := driver.PreCDC(ctx, nil)
	require.NoError(t, err)

	slotLSN := getSlotLSN(t, ctx, db)
	stateLSN := getStateLSN(t, state)

	assert.Equal(t, slotLSN, stateLSN,
		"after PreCDC, OLake state LSN (%s) must equal slot confirmed_flush_lsn (%s)", stateLSN, slotLSN)

	// Verify state file was actually written
	stateFile := viper.GetString(constants.StatePath)
	info, err := os.Stat(stateFile)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "state file should be non-empty")
}

func TestPreCDC_ExistingState_ValidatesLSN(t *testing.T) {
	pg := startEmbeddedPostgres(t)
	defer func() { require.NoError(t, pg.Stop()) }()

	db := connectDB(t)
	defer db.Close()

	setupCDCInfra(t, db)

	ctx := context.Background()
	state := newTestState(t)
	driver := newPostgresDriver(t, db, state)

	// First run: initialize state
	err := driver.PreCDC(ctx, nil)
	require.NoError(t, err)

	// Second run with matching state: should pass validation
	err = driver.PreCDC(ctx, nil)
	assert.NoError(t, err, "PreCDC with matching state should succeed")

	// Simulate the old bug: state has a more recent LSN than the slot
	state.SetGlobal(waljs.WALState{LSN: "F/FFFFFFFF"})

	err = driver.PreCDC(ctx, nil)
	assert.Error(t, err, "PreCDC should fail when state LSN is ahead of slot")
	assert.Contains(t, err.Error(), "lsn mismatch")
}
