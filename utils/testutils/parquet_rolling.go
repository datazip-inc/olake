package testutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// RollingSeedRows is the number of rows the rolling test seeds. With ~100 bytes of
	// incompressible parquet per row this is ~8MB — several times the 1MB roll threshold,
	// so the writer is forced to roll into a handful of files. Shared with the driver's
	// "rolling_seed" ExecuteQuery case so the assertion and the seed stay in lock-step.
	RollingSeedRows = 50000
	// rollingMaxFileBytes must match "max_file_size_mb" in parquet_destination_rolling.json (1 MB).
	rollingMaxFileBytes = 1 * 1024 * 1024
)

// TestParquetRolling exercises the parquet writer's memory/size-based file rolling against a
// real S3 (MinIO) destination. It seeds a single bulk table, syncs it with a tiny
// max_file_size_mb, and asserts the writer split the output into multiple size-bounded files
// without losing rows.
//
// The seeded data (~8MB) is far smaller than the backfill chunk size (EffectiveParquetSize,
// 2GB), so the whole table is one chunk handled by one writer — meaning multiple bounded
// files can only come from rolling, not from chunk fan-out.
func (cfg *IntegrationTest) TestParquetRolling(t *testing.T) {
	ctx := context.Background()
	table := fmt.Sprintf("%s_rolling_test_table", cfg.TestConfig.Driver)

	// Isolated run config: point the sync at the small-threshold destination, and keep the
	// catalog/state in the container's /tmp so it can't race the standard tests that share
	// the testdata bind-mount.
	rc := *cfg.TestConfig
	rc.ParquetDestinationPath = fmt.Sprintf("/test-olake/drivers/%s/internal/testdata/parquet_destination_rolling.json", rc.Driver)
	// Copy the source config into /tmp so discover writes its catalog/state next to it
	// (dir(--config)/streams.json), fully isolated from the shared testdata bind-mount.
	rc.SourcePath = fmt.Sprintf("/tmp/%s_rolling_source.json", rc.Driver)
	rc.CatalogPath = "/tmp/streams.json"
	rc.StatePath = "/tmp/state.json"

	cfg.runInTestContainer(ctx, t, func(ctx context.Context, c testcontainers.Container) error {
		run := func(label, cmd string) error {
			if cmd == "" {
				return nil
			}
			code, out, err := utils.ExecCommand(ctx, c, cmd)
			if err != nil || code != 0 {
				return fmt.Errorf("%s failed (code %d): %v\n%s", label, code, err, string(out))
			}
			return nil
		}

		if err := run("install tooling", installCmd); err != nil {
			return err
		}
		// Isolate the source config under /tmp so discover writes its catalog there
		// (dir(--config)/streams.json) instead of the shared testdata bind-mount.
		if err := run("isolate source config", fmt.Sprintf("cp %s %s", cfg.TestConfig.SourcePath, rc.SourcePath)); err != nil {
			return err
		}

		// Seed a fresh bulk table (host -> postgres).
		cfg.ExecuteQuery(ctx, t, []string{table}, "drop", false)
		cfg.ExecuteQuery(ctx, t, []string{table}, "rolling_create", false)
		cfg.ExecuteQuery(ctx, t, []string{table}, "rolling_seed", false)

		// Discover into the isolated /tmp catalog, force full_refresh (no cursor), and select
		// only this stream with normalization on and no partitioning.
		if err := run("discover", discoverCommand(rc)); err != nil {
			return err
		}
		if err := run("set sync_mode", updateStreamConfigCommand(rc, cfg.Namespace, table, "full_refresh", "")); err != nil {
			return err
		}
		if err := run("select streams", updateSelectedStreamsCommand(rc, cfg.Namespace, "", "", []string{table}, true, "")); err != nil {
			return err
		}

		// Sync to parquet/MinIO with a small buffer so rolled files hug the threshold.
		if err := run("sync", syncCommand(rc, false, "parquet", "--destination-buffer-size", "1000")); err != nil {
			return err
		}
		return nil
	})

	cfg.verifyParquetRolling(t, table)

	// Cleanup: rolled objects in MinIO + the source table.
	if err := DeleteParquetFiles(t, cfg.DestinationDB, table); err != nil {
		t.Logf("cleanup: failed to delete parquet files: %v", err)
	}
	cfg.ExecuteQuery(ctx, t, []string{table}, "drop", false)
}

// verifyParquetRolling reads the rolled objects from MinIO and asserts the writer
// (1) produced multiple files, (2) bounded every file near the roll threshold (the core
// proof — a broken roll would leave one oversized file), and (3) preserved every row.
func (cfg *IntegrationTest) verifyParquetRolling(t *testing.T, table string) {
	t.Helper()
	ctx := context.Background()
	const bucket = "warehouse"
	prefix := fmt.Sprintf("%s/%s/", cfg.DestinationDB, table)

	mc, err := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("admin", "password", ""),
		Secure: false,
	})
	require.NoError(t, err, "failed to create MinIO client")

	var keys []string
	var totalSize int64
	for obj := range mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: false}) {
		require.NoError(t, obj.Err, "error listing MinIO objects")
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		require.LessOrEqualf(t, obj.Size, int64(2*rollingMaxFileBytes),
			"file %s is %d bytes, exceeding 2x the roll threshold — rolling did not bound file size", obj.Key, obj.Size)
		keys = append(keys, obj.Key)
		totalSize += obj.Size
	}

	require.GreaterOrEqualf(t, len(keys), 2,
		"expected the writer to roll into multiple parquet files under %s, got %d", prefix, len(keys))
	require.Greaterf(t, totalSize, int64(4*rollingMaxFileBytes),
		"total parquet size %d is too small to meaningfully exercise rolling", totalSize)

	// Sum footer row counts across every rolled file — no rows may be lost at a roll boundary.
	var totalRows int64
	for _, key := range keys {
		obj, gerr := mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		require.NoErrorf(t, gerr, "failed to get object %s", key)
		data, rerr := io.ReadAll(obj)
		_ = obj.Close()
		require.NoErrorf(t, rerr, "failed to read object %s", key)
		pf, oerr := pqgo.OpenFile(bytes.NewReader(data), int64(len(data)))
		require.NoErrorf(t, oerr, "failed to open parquet %s", key)
		totalRows += pf.NumRows()
	}
	require.Equalf(t, int64(RollingSeedRows), totalRows, "row count mismatch across rolled files")

	t.Logf("parquet rolling OK: %d files, %d bytes total, %d rows (threshold %d bytes)",
		len(keys), totalSize, totalRows, rollingMaxFileBytes)
}
