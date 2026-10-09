package performance

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

const icebergDestinationFile = "iceberg_destination.json"

const (
	// BenchmarkThreshold is the share of the recorded average RPS a run must reach to pass.
	BenchmarkThreshold = 0.9
	maxRPSHistorySize  = 5
)

// TestHandler is one driver's benchmark: the streams it reads and the config that names where they live.
type TestHandler struct {
	*testutils.TestConfig
	BackfillStreams []string
	CDCStreams      []string
}

// validate checks the fields the benchmark itself needs; NewTestConfig has already validated the
// TestConfig by the time one reaches here.
func (th *TestHandler) validate(t *testing.T) {
	t.Helper()
	require.NotNil(t, th.TestConfig, "performance.TestHandler.TestConfig is not set")
	// A benchmark with nothing to read still reports a rate, and it is the rate of doing nothing.
	require.Falsef(t, len(th.BackfillStreams) == 0 && len(th.CDCStreams) == 0,
		"performance.TestHandler declares neither BackfillStreams nor CDCStreams")
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
func (th *TestHandler) TestPerformance(t *testing.T) {
	th.validate(t)
	ctx := t.Context()

	// The CDC configuration a previous run left behind (a slot holding its own WAL, a binlog
	// position) is what the next backfill would have to read past, so start from a clean one.
	if th.Driver == string(constants.Postgres) || th.Driver == string(constants.MySQL) {
		th.ExecuteQuery(ctx, t, th.TestConfig, "reset_cdc_config")
		t.Log("CDC config reset completed")
	}

	if passed := t.Run("Backfill", func(t *testing.T) {
		if err := th.runBackfill(ctx, t); err != nil {
			t.Fatalf("backfill benchmark failed: %s", err)
		}
	}); !passed {
		t.Log("stopping after the backfill phase failed; the CDC phase reads the state it did not write")
		return
	}

	if len(th.CDCStreams) == 0 {
		return
	}
	t.Run("CDC", func(t *testing.T) {
		if err := th.runCDC(ctx, t); err != nil {
			t.Fatalf("cdc benchmark failed: %s", err)
		}
	})
}

// runBackfill measures a full read of BackfillStreams.
func (th *TestHandler) runBackfill(ctx context.Context, t *testing.T) error {
	if err := th.discoverStreams(ctx, th.BackfillStreams); err != nil {
		return err
	}

	// MySQL derives its chunk plan from InnoDB statistics, which drift between runs; seed the
	// committed plan instead so every benchmark measures the same split.
	usePreChunkedState := th.Driver == string(constants.MySQL)
	if usePreChunkedState {
		if err := testutils.CopyFile(th.GetFilePath("performance_state.json"), th.GetFilePath("state.json")); err != nil {
			return fmt.Errorf("failed to seed the pre-chunked state: %s", err)
		}
	}

	defer testutils.TrackPhaseTiming(t, th.Driver, "backfill sync")()
	if out, err := th.timedSync(ctx, usePreChunkedState); err != nil {
		return fmt.Errorf("backfill sync failed: %s\n%s", err, out)
	}

	return th.recordRPS(t, true)
}

// runCDC measures a read of the changes bulk_cdc_data_insert leaves behind. The stateless sync
// before it is what puts the driver's CDC cursor ahead of them.
func (th *TestHandler) runCDC(ctx context.Context, t *testing.T) error {
	th.ExecuteQuery(ctx, t, th.TestConfig, "setup_cdc")

	if err := th.discoverStreams(ctx, th.CDCStreams); err != nil {
		return err
	}

	if code, out, err := th.runOlake(ctx, testutils.SyncArgs(false, icebergDestinationFile, th.destinationPrefix()...)...); err != nil || code != 0 {
		return fmt.Errorf("failed to write the initial CDC state: %s\n%s", err, out)
	}

	th.ExecuteQuery(ctx, t, th.TestConfig, "bulk_cdc_data_insert")

	defer testutils.TrackPhaseTiming(t, th.Driver, "cdc sync")()
	if out, err := th.timedSync(ctx, true); err != nil {
		return fmt.Errorf("cdc sync failed: %s\n%s", err, out)
	}

	return th.recordRPS(t, false)
}

// discoverStreams runs discover and selects the streams the phase measures, so the benchmark reads
// the same catalog a deployed sync would build for itself.
func (th *TestHandler) discoverStreams(ctx context.Context, streams []string) error {
	code, out, err := th.runOlake(ctx, testutils.DiscoverArgs(th.destinationPrefix()...)...)
	if err != nil || code != 0 {
		return fmt.Errorf("discover failed: %s\n%s", err, out)
	}
	if err := testutils.UpdateSelectedStreams(th.TestConfig, th.Namespace, "", "", streams, nil); err != nil {
		return fmt.Errorf("failed to select %s: %s", strings.Join(streams, ", "), err)
	}
	return nil
}

// destinationPrefix names the destination database every phase writes into.
func (th *TestHandler) destinationPrefix() []string {
	return []string{"--destination-database-prefix", fmt.Sprintf("performance_%s", th.Driver)}
}

// runOlake runs the driver image with host networking so the benchmark reaches the external
// instances directly, exactly as a deployed sync would.
func (th *TestHandler) runOlake(ctx context.Context, olakeArgs ...string) (int, []byte, error) {
	args := testutils.DockerRunArgs(th.TestConfig, []string{"--network", "host"}, olakeArgs)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return testutils.DockerExitResult(out, err, olakeArgs[0])
}

// timedSync runs a sync bounded by SyncTimeout. Hitting the window is expected -- this is a bounded
// throughput measurement, not a completeness one -- so the still-running container is stopped and
// whatever it managed reads out of stats.json.
func (th *TestHandler) timedSync(ctx context.Context, useState bool) ([]byte, error) {
	// Named, so a sync that outlives its window can be stopped rather than hunted for.
	name := fmt.Sprintf("olake-perf-%s", th.Driver)
	_ = exec.Command("docker", "rm", "-f", name).Run() // drop any stale container from a previous run

	timedCtx, cancel := context.WithTimeout(ctx, testutils.SyncTimeout)
	defer cancel()

	olakeArgs := testutils.SyncArgs(useState, icebergDestinationFile, th.destinationPrefix()...)
	args := testutils.DockerRunArgs(th.TestConfig, []string{"--network", "host", "--name", name}, olakeArgs)
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
func (th *TestHandler) recordRPS(t *testing.T, isBackfill bool) error {
	rps, err := th.syncedRPS()
	if err != nil {
		return err
	}

	benchmarks, err := loadBenchmarks(th.GetFixturePath("benchmarks.json"))
	if err != nil {
		return err
	}
	averageRPS, observations := benchmarks.stats(isBackfill)
	mode := testutils.Ternary(isBackfill, "backfill", "cdc").(string)
	t.Logf("%s %s: currentRPS %.2f, averageRPS %.2f, observations %d", th.Driver, mode, rps, averageRPS, observations)

	if observations == 0 {
		t.Logf("no benchmarks recorded for %s %s yet, seeding the history with this run", th.Driver, mode)
	} else {
		require.GreaterOrEqualf(t, rps, BenchmarkThreshold*averageRPS,
			"%s %s performance below benchmark: %.2f rps against an average of %.2f", th.Driver, mode, rps, averageRPS)
	}

	return benchmarks.record(isBackfill, rps)
}

// syncedRPS reads the rate the last sync reported, which it writes to stats.json as "<rps> rps".
func (th *TestHandler) syncedRPS() (float64, error) {
	var stats SyncSpeed
	if err := testutils.UnmarshalFile(th.GetFilePath("stats.json"), &stats, false); err != nil {
		return 0, err
	}
	rps, err := testutils.ParseFloat64(strings.Split(stats.Speed, " ")[0])
	if err != nil {
		return 0, fmt.Errorf("failed to read the RPS out of %q: %s", stats.Speed, err)
	}
	return rps, nil
}

// SyncSpeed is the shape of the stats.json a sync writes; its Speed reads "<rps> rps".
type SyncSpeed struct {
	Speed string `json:"Speed"`
}

// history stores the RPS values and the last updated time for a given mode.
type history struct {
	RPS       []float64 `json:"rps"`
	UpdatedAt time.Time `json:"updated_at"`
}

// benchmarkStore stores the benchmark RPS history for backfill and CDC modes.
type benchmarkStore struct {
	Backfill history `json:"backfill"`
	CDC      history `json:"cdc"`
	FilePath string  `json:"-"`
}

// initializes the benchmark store with the given path and loads the stored benchmarks data from the file.
func loadBenchmarks(path string) (*benchmarkStore, error) {
	store := &benchmarkStore{
		Backfill: history{
			RPS:       make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now().UTC(),
		},
		CDC: history{
			RPS:       make([]float64, 0, maxRPSHistorySize),
			UpdatedAt: time.Now().UTC(),
		},
		FilePath: path,
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

// load loads the stored benchmarks data from the file.
func (s *benchmarkStore) load() error {
	if err := testutils.UnmarshalFile(s.FilePath, s, false); err != nil {
		if _, statErr := os.Stat(s.FilePath); os.IsNotExist(statErr) {
			// Missing file is acceptable, it will be created when the first RPS is recorded.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %s", s.FilePath, err)
	}

	return nil
}

// record records a new benchmark RPS value for the given driver and mode, and persists it to the file.
func (s *benchmarkStore) record(isBackfill bool, rps float64) error {
	rpsValues := testutils.Ternary(isBackfill, s.Backfill.RPS, s.CDC.RPS).([]float64)

	rpsValues = append(rpsValues, rps)

	// Truncate history to maintain a rolling window of the last maxRPSHistorySize values.
	if len(rpsValues) > maxRPSHistorySize {
		rpsValues = rpsValues[1:]
	}

	if isBackfill {
		s.Backfill.RPS = rpsValues
		s.Backfill.UpdatedAt = time.Now().UTC()
	} else {
		s.CDC.RPS = rpsValues
		s.CDC.UpdatedAt = time.Now().UTC()
	}

	return testutils.FileLoggerWithPath(s, s.FilePath)
}

// stats returns the average RPS and count of past RPS values for the given driver and mode.
// The count cannot exceed maxRPSHistorySize.
func (s *benchmarkStore) stats(isBackfill bool) (averageRPS float64, observations int) {
	rpsValues := testutils.Ternary(isBackfill, s.Backfill.RPS, s.CDC.RPS).([]float64)

	if len(rpsValues) == 0 {
		// No benchmarks recorded for this mode yet.
		return 0, 0
	}

	return testutils.Average(rpsValues), len(rpsValues)
}
