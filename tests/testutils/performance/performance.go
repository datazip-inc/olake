package performance

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

const icebergDestinationFile = "iceberg_destination.json"

// Test is one driver's benchmark: the streams it reads and the config that names where they live.
type Test struct {
	*testutils.TestConfig
	BackfillStreams []string
	CDCStreams      []string
}

// validate checks the fields the benchmark itself needs; NewTestConfig has already validated the
// TestConfig by the time one reaches here.
func (cfg *Test) validate(t *testing.T) {
	t.Helper()
	require.NotNil(t, cfg.TestConfig, "performance.Test.TestConfig is not set")
	// A benchmark with nothing to read still reports a rate, and it is the rate of doing nothing.
	require.Falsef(t, len(cfg.BackfillStreams) == 0 && len(cfg.CDCStreams) == 0,
		"performance.Test declares neither BackfillStreams nor CDCStreams")
	// TODO: assert BackfillStreams and CDCStreams are disjoint. GetBackfillStreamsFromCDC derives
	// one from the other by trimming "_cdc", so a CDC stream without that suffix passes through
	// unchanged and is counted on both sides of the ratio.
}

// GetBackfillStreamsFromCDC derives the backfill stream names from the CDC ones,
// e.g. "demo_cdc" -> "demo".
func GetBackfillStreamsFromCDC(cdcStreams []string) []string {
	backfillStreams := []string{}
	for _, stream := range cdcStreams {
		backfillStreams = append(backfillStreams, strings.TrimSuffix(stream, "_cdc"))
	}
	return backfillStreams
}

// TestPerformance benchmarks the driver against the instances its source config names: a backfill
// sync first, then a CDC one for the drivers that declare CDC streams. Each phase is asserted
// against the RPS history committed for the driver, then appended to it.
//
// The phases run in sequence rather than parallel: they share the source, the state file and the
// destination, and a benchmark that races another sync measures the contention, not the driver.
func (cfg *Test) TestPerformance(t *testing.T) {
	cfg.validate(t)
	ctx := t.Context()

	// The CDC configuration a previous run left behind (a slot holding its own WAL, a binlog
	// position) is what the next backfill would have to read past, so start from a clean one.
	if cfg.Driver == string(constants.Postgres) || cfg.Driver == string(constants.MySQL) {
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "reset_cdc_config")
		t.Log("CDC config reset completed")
	}

	if !t.Run("Backfill", func(t *testing.T) {
		if err := cfg.runBackfill(ctx, t); err != nil {
			t.Fatalf("backfill benchmark failed: %s", err)
		}
	}) {
		t.Log("stopping after the backfill phase failed; the CDC phase reads the state it did not write")
		return
	}

	if len(cfg.CDCStreams) == 0 {
		return
	}
	t.Run("CDC", func(t *testing.T) {
		if err := cfg.runCDC(ctx, t); err != nil {
			t.Fatalf("cdc benchmark failed: %s", err)
		}
	})
}

// runBackfill measures a full read of BackfillStreams.
func (cfg *Test) runBackfill(ctx context.Context, t *testing.T) error {
	if err := cfg.discoverStreams(ctx, cfg.BackfillStreams); err != nil {
		return err
	}

	// MySQL derives its chunk plan from InnoDB statistics, which drift between runs; seed the
	// committed plan instead so every benchmark measures the same split.
	usePreChunkedState := cfg.Driver == string(constants.MySQL)
	if usePreChunkedState {
		if err := testutils.CopyFile(cfg.GetFilePath("performance_state.json"), cfg.GetFilePath("state.json")); err != nil {
			return fmt.Errorf("failed to seed the pre-chunked state: %s", err)
		}
	}

	defer testutils.TrackPhaseTiming(t, cfg.Driver, "backfill sync")()
	if out, err := cfg.timedSync(ctx, usePreChunkedState); err != nil {
		return fmt.Errorf("backfill sync failed: %s\n%s", err, out)
	}

	return cfg.recordRPS(t, true)
}

// runCDC measures a read of the changes bulk_cdc_data_insert leaves behind. The stateless sync
// before it is what puts the driver's CDC cursor ahead of them.
func (cfg *Test) runCDC(ctx context.Context, t *testing.T) error {
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "setup_cdc")

	if err := cfg.discoverStreams(ctx, cfg.CDCStreams); err != nil {
		return err
	}

	if code, out, err := cfg.runOlake(ctx, testutils.SyncArgs(false, icebergDestinationFile, cfg.destinationPrefix()...)...); err != nil || code != 0 {
		return fmt.Errorf("failed to write the initial CDC state: %s\n%s", err, out)
	}

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "bulk_cdc_data_insert")

	defer testutils.TrackPhaseTiming(t, cfg.Driver, "cdc sync")()
	if out, err := cfg.timedSync(ctx, true); err != nil {
		return fmt.Errorf("cdc sync failed: %s\n%s", err, out)
	}

	return cfg.recordRPS(t, false)
}

// discoverStreams runs discover and selects the streams the phase measures, so the benchmark reads
// the same catalog a deployed sync would build for itself.
func (cfg *Test) discoverStreams(ctx context.Context, streams []string) error {
	code, out, err := cfg.runOlake(ctx, testutils.DiscoverArgs(cfg.destinationPrefix()...)...)
	if err != nil || code != 0 {
		return fmt.Errorf("discover failed: %s\n%s", err, out)
	}
	if err := testutils.UpdateSelectedStreams(cfg.TestConfig, cfg.Namespace, "", "", streams, ""); err != nil {
		return fmt.Errorf("failed to select %s: %s", strings.Join(streams, ", "), err)
	}
	return nil
}

// destinationPrefix names the destination database every phase writes into.
func (cfg *Test) destinationPrefix() []string {
	return []string{"--destination-database-prefix", fmt.Sprintf("performance_%s", cfg.Driver)}
}

// runOlake runs the driver image with host networking so the benchmark reaches the external
// instances directly, exactly as a deployed sync would.
func (cfg *Test) runOlake(ctx context.Context, olakeArgs ...string) (int, []byte, error) {
	args := testutils.DockerRunArgs(cfg.TestConfig, cfg.DriverImage, []string{"--network", "host"}, olakeArgs)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return testutils.DockerExitResult(out, err, olakeArgs[0])
}

// timedSync runs a sync bounded by SyncTimeout. Hitting the window is expected -- this is a bounded
// throughput measurement, not a completeness one -- so the still-running container is stopped and
// whatever it managed reads out of stats.json.
func (cfg *Test) timedSync(ctx context.Context, useState bool) ([]byte, error) {
	// Named, so a sync that outlives its window can be stopped rather than hunted for.
	name := fmt.Sprintf("olake-perf-%s", cfg.Driver)
	_ = exec.Command("docker", "rm", "-f", name).Run() // drop any stale container from a previous run

	timedCtx, cancel := context.WithTimeout(ctx, testutils.SyncTimeout)
	defer cancel()

	olakeArgs := testutils.SyncArgs(useState, icebergDestinationFile, cfg.destinationPrefix()...)
	args := testutils.DockerRunArgs(cfg.TestConfig, cfg.DriverImage, []string{"--network", "host", "--name", name}, olakeArgs)
	out, err := exec.CommandContext(timedCtx, "docker", args...).CombinedOutput()
	if timedCtx.Err() == context.DeadlineExceeded {
		_ = exec.Command("docker", "kill", name).Run()
		return out, nil
	}

	code, out, err := testutils.DockerExitResult(out, err, "sync")
	if err != nil {
		return out, err
	}
	if code != 0 {
		return out, testutils.RenderOlakeFailure(code, nil, nil)
	}
	return out, nil
}

// recordRPS asserts the rate the phase just wrote to stats.json against the driver's history, then
// appends it. A driver with no history yet passes and seeds it, which is how a new one is onboarded.
func (cfg *Test) recordRPS(t *testing.T, isBackfill bool) error {
	rps, err := cfg.syncedRPS()
	if err != nil {
		return err
	}

	benchmarks, err := loadBenchmarks(cfg.GetFixturePath("benchmarks.json"))
	if err != nil {
		return err
	}
	averageRPS, observations := benchmarks.stats(isBackfill)
	mode := testutils.Ternary(isBackfill, "backfill", "cdc").(string)
	t.Logf("%s %s: currentRPS %.2f, averageRPS %.2f, observations %d", cfg.Driver, mode, rps, averageRPS, observations)

	if observations == 0 {
		t.Logf("no benchmarks recorded for %s %s yet, seeding the history with this run", cfg.Driver, mode)
	} else {
		require.GreaterOrEqualf(t, rps, BenchmarkThreshold*averageRPS,
			"%s %s performance below benchmark: %.2f rps against an average of %.2f", cfg.Driver, mode, rps, averageRPS)
	}

	return benchmarks.record(isBackfill, rps)
}

// syncedRPS reads the rate the last sync reported, which it writes to stats.json as "<rps> rps".
func (cfg *Test) syncedRPS() (float64, error) {
	var stats SyncSpeed
	if err := testutils.UnmarshalFile(cfg.GetFilePath("stats.json"), &stats, false); err != nil {
		return 0, err
	}
	rps, err := testutils.ParseFloat64(strings.Split(stats.Speed, " ")[0])
	if err != nil {
		return 0, fmt.Errorf("failed to read the RPS out of %q: %s", stats.Speed, err)
	}
	return rps, nil
}
