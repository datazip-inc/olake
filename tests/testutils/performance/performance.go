package performance

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/stretchr/testify/require"
)

// GetBackfillStreamsFromCDC derives the backfill stream names from the CDC ones,
// e.g. "demo_cdc" -> "demo".
func GetBackfillStreamsFromCDC(cdcStreams []string) []string {
	backfillStreams := []string{}
	for _, stream := range cdcStreams {
		backfillStreams = append(backfillStreams, strings.TrimSuffix(stream, "_cdc"))
	}
	return backfillStreams
}

// seedPerformanceState primes state.json from the pre-chunked seed the driver commits. The seed is
// copied rather than passed to sync directly because sync writes the running state back over
// --state, which would rewrite the working copy the next run reads.
func seedPerformanceState(config *testutils.TestConfig) error {
	return testutils.CopyFile(config.GetFilePath("performance_state.json"), config.GetFilePath("state.json"))
}

func (c *Test) TestPerformance(t *testing.T) {
	if c.Suite == "" {
		c.Suite = "performance"
	} else {
		c.Suite += "_performance"
	}
	c.Validate(t)
	c.Setup(t)
	ctx := t.Context()
	// The perf suite runs against the external DBs source.json describes, so hand every
	// ExecuteQuery their credentials; the integration suites leave this nil (local containers).
	c.TestConfig.SourceBaseConfig = testutils.ReadSourceConfig(t, c.GetFilePath("source.json"))

	// checks if the current rps (from stats.json) is at least 90% of the benchmark rps
	checkBenchmarkRPS := func(config testutils.TestConfig, isBackfill bool) (bool, float64, error) {
		// get current RPS
		var stats SyncSpeed
		if err := testutils.UnmarshalFile(config.GetFilePath("stats.json"), &stats, false); err != nil {
			return false, 0, err
		}
		rps, err := testutils.ParseFloat64(strings.Split(stats.Speed, " ")[0])
		if err != nil {
			return false, 0, fmt.Errorf("failed to get RPS from stats: %s", err)
		}

		// Get past benchmark RPS stats
		benchmarks, err := loadBenchmarks(config.GetFixturePath("benchmarks.json"))
		if err != nil {
			return false, 0, err
		}

		averageRPS, observations := benchmarks.stats(isBackfill)
		t.Logf("currentRPS: %.2f, averageRPS: %.2f, observations: %d", rps, averageRPS, observations)

		// No benchmarks exist yet for this driver/mode
		// Skip validation to allow initial benchmarking.
		if observations == 0 {
			t.Logf("No benchmarks exist yet for %s %s mode, skipping validation", config.Driver, testutils.Ternary(isBackfill, "backfill", "cdc").(string))
			return true, rps, nil
		}
		if rps < BenchmarkThreshold*averageRPS {
			return false, rps, nil
		}
		return true, rps, nil
	}

	recordBenchmark := func(config testutils.TestConfig, isBackfill bool, rps float64) error {
		benchmarks, err := loadBenchmarks(config.GetFixturePath("benchmarks.json"))
		if err != nil {
			return err
		}
		return benchmarks.record(isBackfill, rps)
	}

	// runPerfOlake runs the driver image with host networking so the perf run reaches the
	// external benchmark databases directly, exactly as a deployed sync would.
	runPerfOlake := func(olakeArgs ...string) (int, []byte, error) {
		args := testutils.DockerRunArgs(c.TestConfig, c.DriverImage, []string{"--network", "host"}, olakeArgs)
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		return testutils.DockerExitResult(out, err, olakeArgs[0])
	}

	// syncWithTimeout runs a sync bounded by SyncTimeout. Hitting the window is expected (it is
	// a bounded throughput measurement, not a failure), so the still-running container is stopped.
	syncWithTimeout := func(olakeArgs ...string) ([]byte, error) {
		name := fmt.Sprintf("olake-perf-%s", c.Driver)
		_ = exec.Command("docker", "rm", "-f", name).Run() // drop any stale container from a previous run
		timedCtx, cancel := context.WithTimeout(ctx, testutils.SyncTimeout)
		defer cancel()
		args := testutils.DockerRunArgs(c.TestConfig, c.DriverImage, []string{"--network", "host", "--name", name}, olakeArgs)
		out, err := exec.CommandContext(timedCtx, "docker", args...).CombinedOutput()
		if timedCtx.Err() == context.DeadlineExceeded {
			_ = exec.Command("docker", "kill", name).Run()
			return out, nil
		}
		code, out, derr := testutils.DockerExitResult(out, err, "sync")
		if derr != nil {
			return out, derr
		}
		if code != 0 {
			return out, testutils.SyncFailure(code, nil, nil)
		}
		return out, nil
	}

	t.Run("performance", func(t *testing.T) {
		// reset CDC config
		if c.Driver == string(constants.Postgres) || c.Driver == string(constants.MySQL) {
			c.ExecuteQuery(ctx, t, c.TestConfig, "reset_cdc_config")
			t.Log("CDC config reset completed")
		}

		t.Logf("(backfill) running performance test for %s", c.Driver)

		destDBPrefix := fmt.Sprintf("performance_%s", c.Driver)

		t.Log("(backfill) discover started")
		if code, output, err := runPerfOlake(testutils.DiscoverArgs("--destination-database-prefix", destDBPrefix)...); err != nil || code != 0 {
			t.Fatalf("failed to perform discover:\n%s", string(output))
		}
		t.Log("(backfill) discover completed")

		if err := testutils.UpdateSelectedStreams(c.TestConfig, c.Namespace, "", "", c.BackfillStreams, ""); err != nil {
			t.Fatalf("failed to update streams: %s", err)
		}

		t.Log("(backfill) sync started")
		// MySQL derives its chunk plan from InnoDB statistics, which drift between runs; seed the
		// committed plan instead so every benchmark measures the same split.
		usePreChunkedState := c.Driver == string(constants.MySQL)
		if usePreChunkedState {
			if err := seedPerformanceState(c.TestConfig); err != nil {
				t.Fatalf("failed to seed pre-chunked state from %s: %s", c.GetFilePath("performance_state.json"), err)
			}
		}
		if output, err := syncWithTimeout(testutils.SyncArgs(*c.TestConfig, usePreChunkedState, "iceberg", "--destination-database-prefix", destDBPrefix)...); err != nil {
			t.Fatalf("failed to perform sync:\n%s", string(output))
		}
		t.Log("(backfill) sync completed")

		checkRPS, currentRPS, err := checkBenchmarkRPS(*c.TestConfig, true)
		if err != nil {
			t.Fatalf("failed to check RPS: %s", err)
		}
		require.True(t, checkRPS, fmt.Sprintf("%s backfill performance below benchmark", c.Driver))

		if err := recordBenchmark(*c.TestConfig, true, currentRPS); err != nil {
			t.Fatalf("failed to write RPS history: %s", err)
		}
		t.Logf("✅ SUCCESS: %s backfill", c.Driver)

		if len(c.CDCStreams) > 0 {
			t.Logf("(cdc) running performance test for %s", c.Driver)

			t.Log("(cdc) setup cdc started")
			c.ExecuteQuery(ctx, t, c.TestConfig, "setup_cdc")
			t.Log("(cdc) setup cdc completed")

			t.Log("(cdc) discover started")
			if code, output, err := runPerfOlake(testutils.DiscoverArgs("--destination-database-prefix", destDBPrefix)...); err != nil || code != 0 {
				t.Fatalf("failed to perform discover:\n%s", string(output))
			}
			t.Log("(cdc) discover completed")

			if err := testutils.UpdateSelectedStreams(c.TestConfig, c.Namespace, "", "", c.CDCStreams, ""); err != nil {
				t.Fatalf("failed to update streams: %s", err)
			}

			t.Log("(cdc) state creation started")
			if code, output, err := runPerfOlake(testutils.SyncArgs(*c.TestConfig, false, "iceberg", "--destination-database-prefix", destDBPrefix)...); err != nil || code != 0 {
				t.Fatalf("failed to perform initial sync:\n%s", string(output))
			}
			t.Log("(cdc) state creation completed")

			t.Log("(cdc) trigger cdc started")
			c.ExecuteQuery(ctx, t, c.TestConfig, "bulk_cdc_data_insert")
			t.Log("(cdc) trigger cdc completed")

			t.Log("(cdc) sync started")
			if output, err := syncWithTimeout(testutils.SyncArgs(*c.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix)...); err != nil {
				t.Fatalf("failed to perform CDC sync:\n%s", string(output))
			}
			t.Log("(cdc) sync completed")

			checkRPS, currentRPS, err := checkBenchmarkRPS(*c.TestConfig, false)
			if err != nil {
				t.Fatalf("failed to check RPS: %s", err)
			}
			require.True(t, checkRPS, fmt.Sprintf("%s CDC performance below benchmark", c.Driver))

			if err := recordBenchmark(*c.TestConfig, false, currentRPS); err != nil {
				t.Fatalf("failed to write RPS history: %s", err)
			}
			t.Logf("✅ SUCCESS: %s cdc", c.Driver)
		}
	})
}
