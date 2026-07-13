package testutils

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

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
	if err := UnmarshalFile(s.FilePath, s); err != nil {
		if _, statErr := os.Stat(s.FilePath); os.IsNotExist(statErr) {
			// Missing file is acceptable, it will be created when the first RPS is recorded.
			return nil
		}
		return fmt.Errorf("failed to load rps benchmarks from file %s: %s", s.FilePath, err)
	}

	return nil
}

// record records a new benchmark RPS value for the given driver and mode, and persists it to the file.
func (s *benchmarkStore) record(
	isBackfill bool,
	rps float64,
) error {
	rpsValues := ternary(isBackfill, s.Backfill.RPS, s.CDC.RPS)

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

	return writeJSONFile(s.FilePath, s)
}

// stats returns the average RPS and count of past RPS values for the given driver and mode.
// The count cannot exceed maxRPSHistorySize.
func (s *benchmarkStore) stats(
	isBackfill bool,
) (averageRPS float64, observations int) {
	rpsValues := ternary(isBackfill, s.Backfill.RPS, s.CDC.RPS)

	if len(rpsValues) == 0 {
		// No benchmarks recorded for this mode yet.
		return 0, 0
	}

	return average(rpsValues), len(rpsValues)
}

func (cfg *PerformanceTest) TestPerformance(t *testing.T) {
	ctx := context.Background()

	// checks if the current rps (from stats.json) is at least 90% of the benchmark rps
	checkBenchmarkRPS := func(config TestConfig, isBackfill bool) (bool, float64, error) {
		// get current RPS
		var stats SyncSpeed
		if err := UnmarshalFile(config.HostStatsPath, &stats); err != nil {
			return false, 0, err
		}
		rps, err := strconv.ParseFloat(strings.Split(stats.Speed, " ")[0], 64)
		if err != nil {
			return false, 0, fmt.Errorf("failed to get RPS from stats: %s", err)
		}

		// Get past benchmark RPS stats
		benchmarks, err := loadBenchmarks(config.BenchmarksPath)
		if err != nil {
			return false, 0, err
		}

		averageRPS, observations := benchmarks.stats(isBackfill)
		t.Logf("currentRPS: %.2f, averageRPS: %.2f, observations: %d", rps, averageRPS, observations)

		// No benchmarks exist yet for this driver/mode
		// Skip validation to allow initial benchmarking.
		if observations == 0 {
			t.Logf("No benchmarks exist yet for %s %s mode, skipping validation", config.Driver, ternary(isBackfill, "backfill", "cdc"))
			return true, rps, nil
		}
		if rps < BenchmarkThreshold*averageRPS {
			return false, rps, nil
		}
		return true, rps, nil
	}

	recordBenchmark := func(config TestConfig, isBackfill bool, rps float64) error {
		benchmarks, err := loadBenchmarks(config.BenchmarksPath)
		if err != nil {
			return err
		}
		return benchmarks.record(isBackfill, rps)
	}

	syncWithTimeout := func(ctx context.Context, c testcontainers.Container, cmd string) ([]byte, error) {
		timedCtx, cancel := context.WithTimeout(ctx, SyncTimeout)
		defer cancel()
		code, output, err := ExecCommand(timedCtx, c, cmd)
		// check if sync was canceled due to timeout (expected)
		if timedCtx.Err() == context.DeadlineExceeded {
			killCmd := "pkill -9 -f 'olake.*sync' || true"
			_, _, _ = ExecCommand(ctx, c, killCmd)
			return output, nil
		}
		if err != nil || code != 0 {
			return output, fmt.Errorf("sync failed: %s", err)
		}
		return output, nil
	}

	t.Run("performance", func(t *testing.T) {
		baseImage := ensureTestBaseImage(t, cfg.TestConfig.HostRootPath)
		req := testcontainers.ContainerRequest{
			Image: baseImage,
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", cfg.TestConfig.HostRootPath),
					fmt.Sprintf("%s:/test-olake/drivers/%s/internal/testdata:rw", cfg.TestConfig.HostTestDataPath, cfg.TestConfig.Driver),
					goModCacheMount,
					goBuildCacheMount,
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
				hc.NetworkMode = "host"
			},
			ConfigModifier: func(c *container.Config) {
				c.WorkingDir = "/test-olake"
			},
			Env: map[string]string{
				"TELEMETRY_DISABLED":  "true",
				"OLAKE_SKIP_MOD_TIDY": "1",
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							// reset CDC config
							if cfg.TestConfig.Driver == "postgres" || cfg.TestConfig.Driver == "mysql" {
								cfg.ExecuteQuery(ctx, t, cfg.CDCStreams, "reset_cdc_config", true)
								t.Log("CDC config reset completed")
							}

							t.Logf("(backfill) running performance test for %s", cfg.TestConfig.Driver)

							destDBPrefix := fmt.Sprintf("performance_%s", cfg.TestConfig.Driver)

							t.Log("(backfill) discover started")
							discoverCmd := discoverCommand(*cfg.TestConfig, "--destination-database-prefix", destDBPrefix)
							if code, output, err := ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
								return fmt.Errorf("failed to perform discover:\n%s", string(output))
							}
							t.Log("(backfill) discover completed")

							if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, "", "", cfg.BackfillStreams, ""); err != nil {
								return fmt.Errorf("failed to update streams: %s", err)
							}

							t.Log("(backfill) sync started")
							usePreChunkedState := cfg.TestConfig.Driver == "mysql"
							syncCmd := syncCommand(*cfg.TestConfig, usePreChunkedState, "iceberg", "--destination-database-prefix", destDBPrefix)
							if output, err := syncWithTimeout(ctx, c, syncCmd); err != nil {
								return fmt.Errorf("failed to perform sync:\n%s", string(output))
							}
							t.Log("(backfill) sync completed")

							checkRPS, currentRPS, err := checkBenchmarkRPS(*cfg.TestConfig, true)
							if err != nil {
								return fmt.Errorf("failed to check RPS: %s", err)
							}

							require.True(t, checkRPS, fmt.Sprintf("%s backfill performance below benchmark", cfg.TestConfig.Driver))

							if err := recordBenchmark(*cfg.TestConfig, true, currentRPS); err != nil {
								return fmt.Errorf("failed to write RPS history: %s", err)
							}
							t.Logf("✅ SUCCESS: %s backfill", cfg.TestConfig.Driver)

							if len(cfg.CDCStreams) > 0 {
								t.Logf("(cdc) running performance test for %s", cfg.TestConfig.Driver)

								t.Log("(cdc) setup cdc started")
								cfg.ExecuteQuery(ctx, t, cfg.CDCStreams, "setup_cdc", true)
								t.Log("(cdc) setup cdc completed")

								t.Log("(cdc) discover started")
								discoverCmd := discoverCommand(*cfg.TestConfig, "--destination-database-prefix", destDBPrefix)
								if code, output, err := ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to perform discover:\n%s", string(output))
								}
								t.Log("(cdc) discover completed")

								if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, "", "", cfg.CDCStreams, ""); err != nil {
									return fmt.Errorf("failed to update streams: %s", err)
								}

								t.Log("(cdc) state creation started")
								syncCmd := syncCommand(*cfg.TestConfig, false, "iceberg", "--destination-database-prefix", destDBPrefix)
								if code, output, err := ExecCommand(ctx, c, syncCmd); err != nil || code != 0 {
									return fmt.Errorf("failed to perform initial sync:\n%s", string(output))
								}
								t.Log("(cdc) state creation completed")

								t.Log("(cdc) trigger cdc started")
								cfg.ExecuteQuery(ctx, t, cfg.CDCStreams, "bulk_cdc_data_insert", true)
								t.Log("(cdc) trigger cdc completed")

								t.Log("(cdc) sync started")
								syncCmd = syncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix)
								if output, err := syncWithTimeout(ctx, c, syncCmd); err != nil {
									return fmt.Errorf("failed to perform CDC sync:\n%s", string(output))
								}
								t.Log("(cdc) sync completed")

								checkRPS, currentRPS, err := checkBenchmarkRPS(*cfg.TestConfig, false)
								if err != nil {
									return fmt.Errorf("failed to check RPS: %s", err)
								}
								require.True(t, checkRPS, fmt.Sprintf("%s CDC performance below benchmark", cfg.TestConfig.Driver))

								if err := recordBenchmark(*cfg.TestConfig, false, currentRPS); err != nil {
									return fmt.Errorf("failed to write RPS history: %s", err)
								}
								t.Logf("✅ SUCCESS: %s cdc", cfg.TestConfig.Driver)
							}
							return nil
						},
					},
				},
			},
			Cmd: []string{"tail", "-f", "/dev/null"},
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err, "performance test failed: ", err)
		defer func() {
			// Same as runInTestContainer: PID 1 is `tail -f /dev/null`, which ignores
			// SIGTERM, so skip the default 10s grace period and kill immediately.
			if err := container.Terminate(ctx, testcontainers.StopTimeout(0)); err != nil {
				t.Logf("warning: failed to terminate container: %v", err)
			}
		}()
	})
}
