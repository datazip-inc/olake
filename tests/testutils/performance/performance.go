package performance

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

const icebergDestinationFile = "iceberg_s3_destination.json"

const (
	// RPSThreshold is the share of the recorded average RPS a run must reach to pass.
	RPSThreshold = 0.9
	// MemoryThreshold is the multiple of the recorded average peak and P95 memory a run may reach and still pass.
	MemoryThreshold = 1.1
	// maxHistorySize is how many past runs each recorded series keeps.
	maxHistorySize = 5

	// memorySampleInterval is how often the sync container's memory is read.
	memorySampleInterval = time.Second
	// rssCommand prints the Name and VmRSS (resident memory) lines of every process in the container.
	rssCommand = `grep -s -E '^(Name|VmRSS):' /proc/[0-9]*/status`
)

// memoryUsage is the memory the whole sync used over a run: the resident memory (VmRSS) of every
// process in its container, olake and the Java writer it starts, added up at each read. Peak is the
// highest read, the one closest to an OOM kill; P95 is the 95th percentile of the reads, which leaves
// out short spikes and so moves less between runs. Zero means no read found the sync running.
type memoryUsage struct {
	PeakMiB float64
	P95MiB  float64
}

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
// against the RPS and memory history committed for the driver, then appended to it.
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
	out, usage, err := th.timedSync(ctx, usePreChunkedState)
	// Logged before the error check, so a sync that fails, an OOM kill among them, still reports its memory.
	t.Logf("%s backfill: peak memory %.0f MiB, p95 memory %.0f MiB", th.Driver, usage.PeakMiB, usage.P95MiB)
	if err != nil {
		return fmt.Errorf("backfill sync failed: %s\n%s", err, out)
	}

	return th.recordBenchmark(t, true, usage)
}

// runCDC measures a read of the changes bulk_cdc_data_insert leaves behind. The stateless sync
// before it is what puts the driver's CDC cursor ahead of them.
func (th *TestHandler) runCDC(ctx context.Context, t *testing.T) error {
	th.ExecuteQuery(ctx, t, th.TestConfig, "setup_cdc")

	if err := th.discoverStreams(ctx, th.CDCStreams); err != nil {
		return err
	}

	if code, out, err := th.runOlake(ctx, testutils.SyncArgs(th.TestConfig, false, icebergDestinationFile, th.destinationPrefix()...)...); err != nil || code != 0 {
		return fmt.Errorf("failed to write the initial CDC state: %s\n%s", err, out)
	}

	th.ExecuteQuery(ctx, t, th.TestConfig, "bulk_cdc_data_insert")

	defer testutils.TrackPhaseTiming(t, th.Driver, "cdc sync")()
	out, usage, err := th.timedSync(ctx, true)
	// Logged before the error check, so a sync that fails, an OOM kill among them, still reports its memory.
	t.Logf("%s cdc: peak memory %.0f MiB, p95 memory %.0f MiB", th.Driver, usage.PeakMiB, usage.P95MiB)
	if err != nil {
		return fmt.Errorf("cdc sync failed: %s\n%s", err, out)
	}

	return th.recordBenchmark(t, false, usage)
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
// instances directly, exactly as a deployed sync would. A run still going after SyncTimeout is
// killed and reported as a failure.
func (th *TestHandler) runOlake(ctx context.Context, olakeArgs ...string) (int, []byte, error) {
	name := fmt.Sprintf("olake-perf-%s", th.Driver)
	_ = exec.Command("docker", "rm", "-f", name).Run()

	runCtx, cancel := context.WithTimeout(ctx, testutils.SyncTimeout)
	defer cancel()

	args := testutils.DockerRunArgs(th.TestConfig, []string{"--network", "host", "--name", name}, olakeArgs)
	out, err := exec.CommandContext(runCtx, "docker", args...).CombinedOutput()
	if runCtx.Err() == context.DeadlineExceeded {
		_ = exec.Command("docker", "kill", name).Run()
		return -1, out, fmt.Errorf("olake %s of driver %s timed out after %s", olakeArgs[0], th.Driver, testutils.SyncTimeout)
	}
	return testutils.DockerExitResult(out, err, olakeArgs[0])
}

// timedSync runs a sync bounded by SyncTimeout. Hitting the window is expected -- this is a bounded
// throughput measurement, not a completeness one -- so the still-running container is stopped and
// whatever it managed reads out of stats.json. The previous sync's stats.json is removed first: a
// sync writes it only on its stats ticker, so one that never reports would otherwise be credited
// with the last phase's rate. It also returns the memory the sync used.
func (th *TestHandler) timedSync(ctx context.Context, useState bool) ([]byte, memoryUsage, error) {
	// Named, so a sync that outlives its window can be stopped rather than hunted for.
	name := fmt.Sprintf("olake-perf-%s", th.Driver)
	_ = exec.Command("docker", "rm", "-f", name).Run() // drop any stale container from a previous run
	_ = os.Remove(th.GetFilePath("stats.json"))

	timedCtx, cancel := context.WithTimeout(ctx, testutils.SyncTimeout)
	defer cancel()

	olakeArgs := testutils.SyncArgs(th.TestConfig, useState, icebergDestinationFile, th.destinationPrefix()...)
	args := testutils.DockerRunArgs(th.TestConfig, []string{"--network", "host", "--name", name}, olakeArgs)
	stopTracking := trackMemory(timedCtx, name)
	out, err := exec.CommandContext(timedCtx, "docker", args...).CombinedOutput()
	usage := stopTracking()
	if timedCtx.Err() == context.DeadlineExceeded {
		_ = exec.Command("docker", "kill", name).Run()
		if _, statErr := os.Stat(th.GetFilePath("stats.json")); statErr != nil {
			return out, usage, fmt.Errorf("sync of driver %s was killed after %s without reporting any stats", th.Driver, testutils.SyncTimeout)
		}
		return out, usage, nil
	}

	code, out, err := testutils.DockerExitResult(out, err, "sync")
	if err != nil {
		return out, usage, err
	}
	if code != 0 {
		return out, usage, testutils.RenderOlakeFailure(code, nil, nil)
	}
	return out, usage, nil
}

// trackMemory reads the memory of the named container every memorySampleInterval until stop is
// called, and returns its peak and P95 over the run. A spike shorter than the interval can fall
// between two reads.
func trackMemory(ctx context.Context, container string) (stop func() memoryUsage) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan memoryUsage, 1)
	go func() {
		var samples []float64
		// A read before the container starts or after it exits fails, and its empty output adds nothing.
		read := func(ctx context.Context) {
			out, _ := exec.CommandContext(ctx, "docker", "exec", container, "sh", "-c", rssCommand).Output()
			if total := parseMemory(string(out)); total != 0 {
				samples = append(samples, total)
			}
		}
		ticker := time.NewTicker(memorySampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// A sync stopped at its window is still running here (it is killed only afterwards), so one
				// last read catches its final memory. A sync that exited is already removed and this read
				// finds nothing.
				lastCtx, lastCancel := context.WithTimeout(context.Background(), 10*time.Second)
				read(lastCtx)
				lastCancel()
				done <- summarizeMemory(samples)
				return
			case <-ticker.C:
				read(ctx)
			}
		}
	}()
	return func() memoryUsage {
		cancel()
		return <-done
	}
}

// summarizeMemory returns the peak and the nearest-rank P95 of the reads, sorting them in place.
func summarizeMemory(samples []float64) memoryUsage {
	if len(samples) == 0 {
		return memoryUsage{}
	}
	slices.Sort(samples)
	return memoryUsage{
		PeakMiB: samples[len(samples)-1],
		P95MiB:  samples[int(math.Ceil(0.95*float64(len(samples))))-1],
	}
}

// parseMemory reads rssCommand's output, "<status path>:<field>:\t<value>" per line with each process's
// Name before its VmRSS, into the total resident memory of the container's processes. The shell and
// grep of the read itself are left out.
func parseMemory(out string) (totalMiB float64) {
	names := make(map[string]string)
	for line := range strings.SplitSeq(out, "\n") {
		path, rest, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		field, value, _ := strings.Cut(rest, ":")
		value = strings.TrimSpace(value)
		switch field {
		case "Name":
			names[path] = value
		case "VmRSS":
			if name := names[path]; name == "sh" || name == "grep" {
				continue
			}
			kB, err := testutils.ParseFloat64(strings.TrimSuffix(value, " kB"))
			if err != nil {
				continue
			}
			totalMiB += kB / 1024
		}
	}
	return totalMiB
}

// recordBenchmark asserts the rate the phase just wrote to stats.json and its peak and P95 memory
// against the driver's history, then appends them. A driver with no history yet passes and seeds it,
// which is how a new one is onboarded.
func (th *TestHandler) recordBenchmark(t *testing.T, isBackfill bool, usage memoryUsage) error {
	rps, err := th.syncedRPS()
	if err != nil {
		return err
	}

	benchmarks, err := loadBenchmarks(th.GetFixturePath("benchmarks.json"))
	if err != nil {
		return err
	}
	past := benchmarks.phase(isBackfill)
	averageRPS := testutils.Average(past.RPS)
	mode := testutils.Ternary(isBackfill, "backfill", "cdc").(string)
	t.Logf("%s %s: currentRPS %.2f, averageRPS %.2f, observations %d", th.Driver, mode, rps, averageRPS, len(past.RPS))
	averagePeakMiB, averageP95MiB := testutils.Average(past.PeakMemoryMiB), testutils.Average(past.P95MemoryMiB)
	t.Logf("%s %s: peak memory history %.0f MiB (average %.0f), p95 memory history %.0f MiB (average %.0f)",
		th.Driver, mode, past.PeakMemoryMiB, averagePeakMiB, past.P95MemoryMiB, averageP95MiB)

	if len(past.RPS) == 0 {
		t.Logf("no benchmarks recorded for %s %s yet, seeding the history with this run", th.Driver, mode)
	} else {
		require.GreaterOrEqualf(t, rps, RPSThreshold*averageRPS,
			"%s %s performance below benchmark: %.2f rps against an average of %.2f", th.Driver, mode, rps, averageRPS)
	}

	// Every sync runs for seconds (JVM startup alone), so reading nothing means the sampler is broken
	// and the memory gate would pass silently.
	require.Greaterf(t, usage.PeakMiB, 0.0, "no memory sample read for %s %s", th.Driver, mode)
	// record appends peak and P95 together, so both histories hold the same runs.
	if len(past.PeakMemoryMiB) > 0 {
		require.LessOrEqualf(t, usage.PeakMiB, MemoryThreshold*averagePeakMiB,
			"%s %s peak memory above benchmark: %.0f MiB against an average of %.0f MiB", th.Driver, mode, usage.PeakMiB, averagePeakMiB)
		require.LessOrEqualf(t, usage.P95MiB, MemoryThreshold*averageP95MiB,
			"%s %s p95 memory above benchmark: %.0f MiB against an average of %.0f MiB", th.Driver, mode, usage.P95MiB, averageP95MiB)
	}

	return benchmarks.record(isBackfill, rps, usage)
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

// history stores the RPS values, the peak and P95 memory values and the last updated time for a given mode.
type history struct {
	RPS           []float64 `json:"rps"`
	PeakMemoryMiB []float64 `json:"peak_memory_mib,omitempty"`
	P95MemoryMiB  []float64 `json:"p95_memory_mib,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// benchmarkStore stores the benchmark RPS and memory history for backfill and CDC modes.
type benchmarkStore struct {
	Backfill history `json:"backfill"`
	CDC      history `json:"cdc"`
	FilePath string  `json:"-"`
}

// initializes the benchmark store with the given path and loads the stored benchmarks data from the file.
func loadBenchmarks(path string) (*benchmarkStore, error) {
	store := &benchmarkStore{
		Backfill: history{
			RPS:       make([]float64, 0, maxHistorySize),
			UpdatedAt: time.Now().UTC(),
		},
		CDC: history{
			RPS:       make([]float64, 0, maxHistorySize),
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
			// Missing file is acceptable, it will be created when the first run is recorded.
			return nil
		}
		return fmt.Errorf("failed to load benchmarks from file %s: %s", s.FilePath, err)
	}

	return nil
}

// record records a new benchmark RPS value and peak and P95 memory for the given driver and mode,
// and persists them to the file.
func (s *benchmarkStore) record(isBackfill bool, rps float64, usage memoryUsage) error {
	h := s.phase(isBackfill)

	h.RPS = appendToWindow(h.RPS, rps)
	h.PeakMemoryMiB = appendToWindow(h.PeakMemoryMiB, usage.PeakMiB)
	h.P95MemoryMiB = appendToWindow(h.P95MemoryMiB, usage.P95MiB)
	h.UpdatedAt = time.Now().UTC()

	return testutils.FileLoggerWithPath(s, s.FilePath)
}

// appendToWindow appends value, truncating to a rolling window of the last maxHistorySize values.
func appendToWindow(values []float64, value float64) []float64 {
	values = append(values, value)
	if len(values) > maxHistorySize {
		values = values[1:]
	}
	return values
}

// phase returns the history of the given mode. Each series in it holds at most maxHistorySize values.
func (s *benchmarkStore) phase(isBackfill bool) *history {
	return testutils.Ternary(isBackfill, &s.Backfill, &s.CDC).(*history)
}
