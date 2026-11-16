package mysql

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
	"github.com/jmoiron/sqlx"
	"github.com/minio/minio-go/v7"
	"github.com/parquet-go/parquet-go"
)

func TestMySQLGTIDFailover(t *testing.T) {
	if os.Getenv("EXPECT_VERSION") == "5.7" {
		t.Skip("the dedicated MySQL 8.0 pair runs in the default MySQL matrix job")
	}
	t.Run("migrate and promote", testGTIDPromotion)
	t.Run("anonymous transactions", testGTIDAnonymousTransactions)
}

func testGTIDPromotion(t *testing.T) {
	f := newGTIDFailoverTest(t)
	insert := fmt.Sprintf("INSERT INTO %s (id) VALUES ", f.table)
	require.NoError(t, f.sync(t, false))
	initial := f.checkpoint(t)
	require.True(t, initial.GTIDSet == nil)
	f.assertIDs(t, 1)

	// Drain file-mode CDC into a fresh file whose transactions all have GTIDs.
	f.setGTIDMode(t, "OFF_PERMISSIVE")
	f.setGTIDMode(t, "ON_PERMISSIVE")
	f.exec(t, f.primary, "FLUSH BINARY LOGS", insert+"(2)")
	require.NoError(t, f.sync(t, true))
	file := f.checkpoint(t)
	require.True(t, file.GTIDSet == nil)
	require.NotEqualf(t, initial.Position.Name, file.Position.Name, "file sync did not reach the rotated log")
	f.assertIDs(t, 1, 2)

	f.setGTIDMode(t, "ON")
	f.exec(t, f.replica, "STOP REPLICA", "CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION=1", "START REPLICA")
	f.exec(t, f.primary, insert+"(3)")
	require.NoError(t, f.sync(t, true))
	f.assertGTIDCheckpoint(t, f.primary)
	f.assertIDs(t, 1, 2, 3)

	f.exec(t, f.primary, insert+"(4)")
	require.NoError(t, f.sync(t, true))
	primary := f.assertGTIDCheckpoint(t, f.primary)
	f.assertIDs(t, 1, 2, 3, 4)

	var timedOut int
	require.NoError(t, f.replica.GetContext(t.Context(), &timedOut, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", *primary.GTIDSet))
	require.Equal(t, 0, timedOut, "replica did not catch up before promotion")
	var replicaIDs []int
	require.NoError(t, f.replica.SelectContext(t.Context(), &replicaIDs, "SELECT id FROM "+f.table+" ORDER BY id"))
	require.Equal(t, []int{1, 2, 3, 4}, replicaIDs, "promotion must preserve pre-GTID data too")
	f.exec(t, f.replica, "STOP REPLICA", "RESET REPLICA ALL", insert+"(5)")
	f.useSource(t, f.replica)
	require.NoError(t, f.sync(t, true))
	promoted := f.assertGTIDCheckpoint(t, f.replica)
	require.NotEqualf(t, primary.ServerUUID, promoted.ServerUUID, "sync did not switch server")
	require.NotEqualf(t, primary.Position.Name, promoted.Position.Name, "sync reused the primary's file")
	f.assertIDs(t, 1, 2, 3, 4, 5)
	require.NoError(t, f.sync(t, true))
	require.Equal(t, promoted, f.checkpoint(t))
	f.assertIDs(t, 1, 2, 3, 4, 5)
}

func testGTIDAnonymousTransactions(t *testing.T) {
	f := newGTIDFailoverTest(t)
	insert := fmt.Sprintf("INSERT INTO %s (id) VALUES ", f.table)
	require.NoError(t, f.sync(t, false))
	saved := f.checkpoint(t)
	f.exec(t, f.primary, insert+"(2)")
	f.setGTIDMode(t, "OFF_PERMISSIVE")
	f.setGTIDMode(t, "ON_PERMISSIVE")
	f.setGTIDMode(t, "ON")
	f.exec(t, f.primary, "FLUSH BINARY LOGS")

	// Rotating does not make an unread anonymous transaction available under ON.
	err := f.sync(t, true)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "ON_PERMISSIVE")
	require.Equal(t, saved, f.checkpoint(t))
	f.assertIDs(t, 1)

	// Drain the unread row and leave a processed anonymous prefix in the saved file.
	f.setGTIDMode(t, "ON_PERMISSIVE")
	conn, err := f.primary.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	for _, query := range []string{"SET GTID_NEXT='ANONYMOUS'", insert + "(3)", "SET GTID_NEXT='AUTOMATIC'"} {
		_, err = conn.ExecContext(t.Context(), query)
		require.NoError(t, err)
	}
	require.NoError(t, f.sync(t, true))
	f.assertIDs(t, 1, 2, 3)
	require.True(t, f.checkpoint(t).GTIDSet == nil)

	f.setGTIDMode(t, "ON")
	f.exec(t, f.primary, "FLUSH BINARY LOGS", insert+"(4)")
	require.NoError(t, f.sync(t, true))
	require.True(t, f.checkpoint(t).GTIDSet == nil, "finish this run by file position")
	f.assertIDs(t, 1, 2, 3, 4)
	require.NoError(t, f.sync(t, true))
	f.assertGTIDCheckpoint(t, f.primary)
	f.assertIDs(t, 1, 2, 3, 4)
}

type failoverPosition struct {
	Name string
	Pos  uint32
}

type failoverCheckpoint struct {
	Position   failoverPosition `json:"position"`
	ServerUUID string           `json:"server_uuid"`
	GTIDSet    *string          `json:"gtid_set"`
}

type gtidFailoverTest struct {
	cfg              *testutils.TestConfig
	primary, replica *sqlx.DB
	ports            map[*sqlx.DB]int
	table            string
}

func newGTIDFailoverTest(t *testing.T) *gtidFailoverTest {
	t.Helper()
	cfg := mysqlTestConfig(t)
	f := &gtidFailoverTest{cfg: cfg, table: cfg.GetTableName(), ports: make(map[*sqlx.DB]int)}
	project := fmt.Sprintf("olake-%s-%d", cfg.Suite, os.Getpid())
	compose := func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		args = append([]string{"compose", "-p", project, "-f", cfg.GetFixturePath("gtid-compose.yml")}, args...)
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		require.NoError(t, err, "docker compose %v: %s", args, out)
		return strings.TrimSpace(string(out))
	}
	t.Cleanup(func() { compose("down", "--volumes") })
	compose("up", "-d", "--wait")
	connect := func(service string) *sqlx.DB {
		t.Helper()
		_, port, err := net.SplitHostPort(compose("port", service, "3306"))
		require.NoError(t, err)
		db, err := sqlx.Connect("mysql", fmt.Sprintf("root:root1234@tcp(127.0.0.1:%s)/%s", port, cfg.Namespace))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		f.ports[db], err = strconv.Atoi(port)
		require.NoError(t, err)
		return db
	}
	f.primary, f.replica = connect("primary"), connect("replica")

	// Start replication before creating the table: auto-position alone cannot seed pre-GTID rows.
	pos := f.primaryPosition(t)
	f.exec(t, f.replica, fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='primary', SOURCE_USER='root', "+
		"SOURCE_PASSWORD='root1234', SOURCE_LOG_FILE='%s', SOURCE_LOG_POS=%d, GET_SOURCE_PUBLIC_KEY=1", pos.Name, pos.Pos), "START REPLICA")
	f.exec(t, f.primary, "CREATE TABLE "+f.table+" (id BIGINT PRIMARY KEY)", "INSERT INTO "+f.table+" VALUES (1)")
	f.useSource(t, f.primary)
	code, out, err := testutils.RunOlake(t.Context(), t, cfg, testutils.DiscoverArgs("--destination", testutils.ContainerPath("parquet_s3_destination.json"))...)
	if err != nil || code != 0 {
		t.Fatal(testutils.RenderOlakeFailure(code, err, out))
	}
	cfg.DestinationDB = fmt.Sprintf("%s_%d", cfg.UniqueID(), os.Getpid())
	require.NoError(t, cfg.Catalog().Edit(func(doc map[string]any) error {
		selected := testutils.SelectedStreamEntry(doc, cfg.Namespace, f.table)
		if selected == nil {
			return fmt.Errorf("discover did not select %s.%s", cfg.Namespace, f.table)
		}
		selected["sync_mode"], selected["normalization"], selected["destination_database"] = "cdc", true, cfg.DestinationDB
		return nil
	}))
	t.Cleanup(func() {
		if !testutils.KeepTestData() {
			require.NoError(t, testutils.DeleteParquetTable(t, cfg.DestinationDB, f.table))
		}
	})
	return f
}

func (f *gtidFailoverTest) primaryPosition(t *testing.T) failoverPosition {
	t.Helper()
	var pos failoverPosition
	var ignored string
	require.NoError(t, f.primary.QueryRowContext(t.Context(), "SHOW MASTER STATUS").Scan(&pos.Name, &pos.Pos, &ignored, &ignored, &ignored))
	return pos
}

func (f *gtidFailoverTest) setGTIDMode(t *testing.T, mode string) {
	t.Helper()
	if mode == "ON" {
		// No anonymous transaction may still be executing when the replica enters ON.
		pos := f.primaryPosition(t)
		var waited int
		require.NoError(t, f.replica.GetContext(t.Context(), &waited, "SELECT SOURCE_POS_WAIT(?, ?, 30)", pos.Name, pos.Pos))
		require.GreaterOrEqualf(t, waited, 0, "replica did not reach %s:%d", pos.Name, pos.Pos)
	}
	for _, db := range []*sqlx.DB{f.replica, f.primary} {
		f.exec(t, db, "SET GLOBAL gtid_mode='"+mode+"'")
	}
}

func (f *gtidFailoverTest) exec(t *testing.T, db *sqlx.DB, queries ...string) {
	t.Helper()
	for _, query := range queries {
		_, err := db.ExecContext(t.Context(), query)
		require.NoError(t, err, "%s", query)
	}
}

func (f *gtidFailoverTest) useSource(t *testing.T, db *sqlx.DB) {
	t.Helper()
	source := f.cfg.SourceBaseConfig
	source["port"], source["username"], source["password"] = f.ports[db], "root", "root1234"
	source["backoff_retry_count"] = 1
	require.NoError(t, testutils.FileLoggerWithPath(source, f.cfg.GetFilePath("source.json")))
}

func (f *gtidFailoverTest) sync(t *testing.T, useState bool) error {
	t.Helper()
	return testutils.RunSync(t.Context(), t, f.cfg, "parquet_s3_destination.json", useState)
}

func (f *gtidFailoverTest) checkpoint(t *testing.T) failoverCheckpoint {
	t.Helper()
	var state struct {
		Global struct {
			State struct{ State failoverCheckpoint }
		}
	}
	require.NoError(t, testutils.UnmarshalFile(f.cfg.GetFilePath("state.json"), &state, false))
	require.NotEmpty(t, state.Global.State.State.Position.Name)
	return state.Global.State.State
}

func (f *gtidFailoverTest) assertGTIDCheckpoint(t *testing.T, db *sqlx.DB) failoverCheckpoint {
	t.Helper()
	checkpoint := f.checkpoint(t)
	require.NotNil(t, checkpoint.GTIDSet)
	require.NotEmpty(t, *checkpoint.GTIDSet)
	var uuid string
	var equal bool
	query := "SELECT @@server_uuid, GTID_SUBSET(?, @@global.gtid_executed) AND GTID_SUBSET(@@global.gtid_executed, ?)"
	require.NoError(t, db.QueryRowContext(t.Context(), query, *checkpoint.GTIDSet, *checkpoint.GTIDSet).Scan(&uuid, &equal))
	require.Equal(t, uuid, checkpoint.ServerUUID)
	require.True(t, equal, "checkpoint must cover exactly the transactions executed on the current server")
	return checkpoint
}

func (f *gtidFailoverTest) assertIDs(t *testing.T, want ...int64) {
	t.Helper()
	client, err := testutils.NewMinIOClient()
	require.NoError(t, err)
	objects, err := testutils.ListParquetObjects(t.Context(), client, f.cfg.DestinationDB, f.table)
	require.NoError(t, err)
	var ids []int64
	for _, object := range objects {
		reader, err := client.GetObject(t.Context(), testutils.ParquetTestBucket, object.Key, minio.GetObjectOptions{})
		require.NoError(t, err)
		rows, err := parquet.Read[struct {
			ID int64 `parquet:"id"`
		}](reader, object.Size)
		closeErr := reader.Close()
		require.NoError(t, err)
		require.NoError(t, closeErr)
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
	}
	slices.Sort(ids)
	require.Equal(t, want, ids)
}
