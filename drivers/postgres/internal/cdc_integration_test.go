package driver

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/types"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPort        = 15432
	testSlot        = "olake_test_slot"
	testPublication = "olake_test_pub"
	testTable       = "public.cdc_test"
)

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

func connectDB(t *testing.T) *sqlx.DB {
	t.Helper()

	connStr := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", testPort)
	db, err := sqlx.Connect("postgres", connStr)
	require.NoError(t, err)
	return db
}

func setupCDCInfra(t *testing.T, db *sqlx.DB) {
	t.Helper()

	_, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, val TEXT)", testTable))
	require.NoError(t, err, "create table")

	_, err = db.Exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", testPublication, testTable))
	require.NoError(t, err, "create publication")

	var slotName, consistentPoint string
	err = db.QueryRow(fmt.Sprintf(
		"SELECT * FROM pg_create_logical_replication_slot('%s', 'pgoutput')", testSlot,
	)).Scan(&slotName, &consistentPoint)
	require.NoError(t, err, "create replication slot")
}

func newTestPostgresDriver(t *testing.T, db *sqlx.DB) *Postgres {
	t.Helper()

	// State file must be writable or LogState() calls logger.Fatalf → os.Exit.
	stateFile := filepath.Join(t.TempDir(), "state.json")
	viper.Set(constants.StatePath, stateFile)

	connURL := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", "postgres"),
		Host:     fmt.Sprintf("localhost:%d", testPort),
		Path:     "postgres",
		RawQuery: "sslmode=disable",
	}

	return &Postgres{
		client:     db,
		CDCSupport: true,
		config:     &Config{Connection: connURL},
		cdcConfig: CDC{
			ReplicationSlot: testSlot,
			InitialWaitTime: 120,
			Publication:     testPublication,
		},
		state: &types.State{
			Type:    types.GlobalType,
			RWMutex: &sync.RWMutex{},
		},
	}
}

// getSlotLSN reads the current confirmed_flush_lsn of the test replication slot.
func getSlotLSN(t *testing.T, db *sqlx.DB) pglogrepl.LSN {
	t.Helper()

	slot, err := waljs.GetSlotPosition(context.Background(), db, testSlot)
	require.NoError(t, err)
	return slot.LSN
}

// noopCallback is a CDC callback that discards all change events.
func noopCallback(_ context.Context, _ abstract.CDCChange) error {
	return nil
}

// TestPostCDC_FailsWhenSlotAdvancedPastClientXLogPos reproduces a production
// bug where PostCDC's AcknowledgeLSN times out with "LSN not updated after
// 5 minutes".
//
// Production scenario (observed in our OLake deployment):
//
//  1. PreCDC calls pg_replication_slot_advance() to pg_current_wal_lsn(),
//     which is the WAL *write* position.
//
//  2. StreamChanges opens a new replication connection. IdentifySystem returns
//     XLogPos which is the WAL *flush* position — this can lag behind the
//     write position on a busy server. This becomes CurrentWalPosition.
//
//  3. During replication, PrimaryKeepaliveMessages carry ServerWALEnd (also
//     the flush position). The handler blindly assigns:
//     ClientXLogPos = pkm.ServerWALEnd  (pgoutput.go:97, waljs.go:89)
//     This can move ClientXLogPos backwards relative to confirmed_flush_lsn.
//
//  4. StreamChanges exits when ClientXLogPos >= CurrentWalPosition.
//
//  5. PostCDC calls AcknowledgeLSN(fakeAck=false) which sends
//     StandbyStatusUpdate with ClientXLogPos. PostgreSQL ignores the update
//     because confirmed_flush_lsn cannot go backwards. The equality poll
//     (slot.LSN == walPosition) never matches → 5-minute timeout.
//
// From our production logs:
//
//	state LSN:             E/D8EA31B8  (from pg_current_wal_lsn via PreCDC)
//	confirmed_flush_lsn:   E/D8EA31B8
//	IdentifySystem XLogPos: E/D8E19108  (flush position, lower)
//	keepalive ServerWALEnd: E/D8E19108  → overwrites ClientXLogPos
//	PostCDC sends:          E/D8E19108  → PG ignores (below confirmed_flush)
//	→ fatal: "LSN not updated after 5 minutes"
//
// This test reproduces the scenario by advancing the replication slot past
// where StreamChanges will land, then running PostCDC.
func TestPostCDC_FailsWhenSlotAdvancedPastClientXLogPos(t *testing.T) {
	pg := startEmbeddedPostgres(t)
	defer func() { require.NoError(t, pg.Stop()) }()

	db := connectDB(t)
	defer db.Close()

	setupCDCInfra(t, db)
	ctx := context.Background()
	driver := newTestPostgresDriver(t, db)

	// Insert initial data so there's WAL to consume.
	for i := 0; i < 20; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (val) VALUES ('init-%d')", testTable, i))
		require.NoError(t, err)
	}

	// Run PreCDC to initialize the replication state.
	// This advances the slot to pg_current_wal_lsn() (write position).
	err := driver.PreCDC(ctx, nil)
	require.NoError(t, err, "PreCDC should succeed")

	slotAfterPreCDC := getSlotLSN(t, db)
	t.Logf("slot confirmed_flush_lsn after PreCDC: %s", slotAfterPreCDC)

	// Insert more data. This pushes the WAL write position further ahead.
	// When StreamChanges opens a new replication connection, IdentifySystem
	// returns the flush position at that point — potentially below where
	// PreCDC advanced the slot.
	for i := 0; i < 50; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (val) VALUES ('extra-%d')", testTable, i))
		require.NoError(t, err)
	}

	// Now advance the slot even further to simulate what happens across
	// multiple CDC cycles: the slot is at a position that is ahead of where
	// the next IdentifySystem will report.
	var currentWAL string
	err = db.QueryRow("SELECT pg_current_wal_lsn()::text").Scan(&currentWAL)
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("SELECT * FROM pg_replication_slot_advance('%s', '%s')", testSlot, currentWAL))
	require.NoError(t, err)

	slotAdvanced := getSlotLSN(t, db)
	t.Logf("slot confirmed_flush_lsn after advance: %s", slotAdvanced)

	// Insert more data AFTER the advance so the WAL tip moves ahead,
	// but the flush position reported by IdentifySystem may still be
	// at or near the slot position.
	for i := 0; i < 20; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (val) VALUES ('post-%d')", testTable, i))
		require.NoError(t, err)
	}

	// Open a replication connection and create a replicator, same as
	// StreamChanges does internally. This calls IdentifySystem to get
	// the target WAL position.
	connURL := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", "postgres"),
		Host:     fmt.Sprintf("localhost:%d", testPort),
		Path:     "postgres",
		RawQuery: "sslmode=disable",
	}

	slot, err := waljs.GetSlotPosition(ctx, db, testSlot)
	require.NoError(t, err)

	waljsConfig := &waljs.Config{
		Connection:          connURL,
		ReplicationSlotName: testSlot,
		InitialWaitTime:     120 * time.Second,
		Tables:              types.NewSet[types.StreamInterface](),
		Publication:         testPublication,
	}

	replicator, err := waljs.NewReplicator(ctx, waljsConfig, slot, nil, nil)
	require.NoError(t, err)

	socket := replicator.Socket()
	t.Logf("ConfirmedFlushLSN (from slot): %s", socket.ConfirmedFlushLSN)
	t.Logf("CurrentWalPosition (IdentifySystem): %s", socket.CurrentWalPosition)
	t.Logf("ClientXLogPos (initial): %s", socket.ClientXLogPos)

	// The core of the bug: simulate what happens when a keepalive message
	// sets ClientXLogPos to a value behind the slot's confirmed_flush_lsn.
	//
	// In production, pgoutput.go:97 does exactly this:
	//   p.socket.ClientXLogPos = pkm.ServerWALEnd
	//
	// We set it to the IdentifySystem position (CurrentWalPosition), which
	// is the same value that ServerWALEnd carries in keepalive messages.
	// If CurrentWalPosition < ConfirmedFlushLSN, we've reproduced the bug.
	if socket.CurrentWalPosition < socket.ConfirmedFlushLSN {
		// Natural reproduction: IdentifySystem already returned a value
		// behind the slot. This is the exact production scenario.
		socket.ClientXLogPos = socket.CurrentWalPosition
		t.Logf("natural reproduction: CurrentWalPosition (%s) < ConfirmedFlushLSN (%s)",
			socket.CurrentWalPosition, socket.ConfirmedFlushLSN)
	} else {
		// On embedded postgres (single server, low load), the write and
		// flush positions are typically the same. Force the condition by
		// setting ClientXLogPos to an earlier position, same as what the
		// keepalive handler does when ServerWALEnd is stale.
		socket.ClientXLogPos = socket.ConfirmedFlushLSN - 1
		t.Logf("forced reproduction: set ClientXLogPos to %s (behind ConfirmedFlushLSN %s)",
			socket.ClientXLogPos, socket.ConfirmedFlushLSN)
	}

	require.True(t, socket.ClientXLogPos < socket.ConfirmedFlushLSN,
		"precondition: ClientXLogPos (%s) must be behind ConfirmedFlushLSN (%s)",
		socket.ClientXLogPos, socket.ConfirmedFlushLSN)

	// Wire up the replicator into the driver so PostCDC can use it.
	driver.replicator = replicator

	// PostCDC should handle backwards ClientXLogPos gracefully — either by
	// skipping the ack, clamping the position, or using >= in the poll.
	//
	// The current implementation times out because:
	//   - It sends StandbyStatusUpdate with ClientXLogPos (behind the slot)
	//   - PostgreSQL ignores backwards movement of confirmed_flush_lsn
	//   - The equality poll (slot.LSN == walPosition) never matches
	//
	// We use a short timeout to avoid waiting the full 5 minutes.
	postCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err = driver.PostCDC(postCtx, 0)
	assert.NoError(t, err,
		"PostCDC should succeed even when ClientXLogPos is behind confirmed_flush_lsn; "+
			"currently fails with 'LSN not updated after 5 minutes'")
}

// TestFullCDCCycle_PostCDCSucceeds verifies the happy path: a full CDC cycle
// where ClientXLogPos naturally ends up at or ahead of confirmed_flush_lsn.
func TestFullCDCCycle_PostCDCSucceeds(t *testing.T) {
	pg := startEmbeddedPostgres(t)
	defer func() { require.NoError(t, pg.Stop()) }()

	db := connectDB(t)
	defer db.Close()

	setupCDCInfra(t, db)
	ctx := context.Background()

	driver := newTestPostgresDriver(t, db)

	// Insert data before PreCDC so the slot has a meaningful starting position.
	for i := 0; i < 10; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (val) VALUES ('row-%d')", testTable, i))
		require.NoError(t, err)
	}

	err := driver.PreCDC(ctx, nil)
	require.NoError(t, err)

	slotBefore := getSlotLSN(t, db)
	t.Logf("slot confirmed_flush_lsn after PreCDC: %s", slotBefore)

	// Insert data that StreamChanges will consume.
	for i := 10; i < 30; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (val) VALUES ('row-%d')", testTable, i))
		require.NoError(t, err)
	}

	// Run StreamChanges through the driver's full code path.
	metadataStates := make(map[string]any)
	_, err = driver.StreamChanges(ctx, 0, metadataStates, noopCallback)
	require.NoError(t, err, "StreamChanges should succeed")

	socket := driver.replicator.Socket()
	t.Logf("ClientXLogPos after StreamChanges: %s", socket.ClientXLogPos)
	t.Logf("ConfirmedFlushLSN: %s", socket.ConfirmedFlushLSN)

	// Verify ClientXLogPos >= ConfirmedFlushLSN in the happy path.
	assert.GreaterOrEqual(t, uint64(socket.ClientXLogPos), uint64(socket.ConfirmedFlushLSN),
		"in the happy path, ClientXLogPos should be at or ahead of confirmed_flush_lsn")

	postCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err = driver.PostCDC(postCtx, 0)
	assert.NoError(t, err, "PostCDC should succeed in the happy path")

	slotAfter := getSlotLSN(t, db)
	t.Logf("slot confirmed_flush_lsn after PostCDC: %s", slotAfter)
	assert.GreaterOrEqual(t, uint64(slotAfter), uint64(slotBefore),
		"slot should advance after a successful CDC cycle")
}

// TestAcknowledgeLSN_RejectsBackwardsPosition directly tests that
// AcknowledgeLSN handles the case where the reported position is behind
// the slot's confirmed_flush_lsn. This is the minimal reproduction of
// the bug at the waljs layer.
func TestAcknowledgeLSN_RejectsBackwardsPosition(t *testing.T) {
	pg := startEmbeddedPostgres(t)
	defer func() { require.NoError(t, pg.Stop()) }()

	db := connectDB(t)
	defer db.Close()

	setupCDCInfra(t, db)
	ctx := context.Background()

	// Generate WAL and advance the slot.
	for i := 0; i < 50; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (val) VALUES ('row-%d')", testTable, i))
		require.NoError(t, err)
	}

	var currentLSN string
	err := db.QueryRow("SELECT pg_current_wal_lsn()::text").Scan(&currentLSN)
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf("SELECT * FROM pg_replication_slot_advance('%s', '%s')", testSlot, currentLSN))
	require.NoError(t, err)

	slot := getSlotLSN(t, db)
	t.Logf("slot confirmed_flush_lsn: %s", slot)

	// Position ClientXLogPos behind the slot, as the keepalive handler does.
	behindLSN, err := pglogrepl.ParseLSN("0/1000000")
	require.NoError(t, err)
	require.True(t, behindLSN < slot,
		"precondition: behindLSN (%s) must be behind slot (%s)", behindLSN, slot)

	// Create a replicator to get a socket with a valid replication connection.
	connURL := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", "postgres"),
		Host:     fmt.Sprintf("localhost:%d", testPort),
		Path:     "postgres",
		RawQuery: "sslmode=disable",
	}
	waljsConfig := &waljs.Config{
		Connection:          connURL,
		ReplicationSlotName: testSlot,
		InitialWaitTime:     120 * time.Second,
		Tables:              types.NewSet[types.StreamInterface](),
		Publication:         testPublication,
	}

	slotPos, err := waljs.GetSlotPosition(ctx, db, testSlot)
	require.NoError(t, err)

	replicator, err := waljs.NewReplicator(ctx, waljsConfig, slotPos, nil, nil)
	require.NoError(t, err)
	defer waljs.Cleanup(ctx, replicator.Socket())

	// Overwrite ClientXLogPos to the behind position, simulating keepalive.
	replicator.Socket().ClientXLogPos = behindLSN

	ackCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err = waljs.AcknowledgeLSN(ackCtx, db, replicator.Socket(), false)
	assert.NoError(t, err,
		"AcknowledgeLSN should handle backwards ClientXLogPos gracefully; "+
			"currently fails because PG won't move confirmed_flush_lsn backwards "+
			"and the equality poll never matches")
}
