package testutils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

type TestConfig struct {
	Driver          string
	HostRoot        string
	SourcePath      string
	CatalogPath     string
	DestinationPath string
	StatePath       string
	StatsPath       string
}

type PerformanceTestConfig struct {
	TestConfig      *TestConfig
	Namespace       string
	BackfillStreams []string
	CDCStreams      []string
	ConnectDB       func(ctx context.Context) (interface{}, error)
	CloseDB         func(conn interface{}) error
	SetupCDC        func(ctx context.Context, conn interface{}) error
	TriggerCDC      func(ctx context.Context, conn interface{}) error
	SupportsCDC     bool
}

func GetTestConfig(driver string) *TestConfig {
	pwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	hostRoot := filepath.Join(pwd, "../../..")

	return &TestConfig{
		Driver:          driver,
		HostRoot:        hostRoot,
		SourcePath:      fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/source.json", driver),
		CatalogPath:     fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/streams.json", driver),
		DestinationPath: fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/destination.json", driver),
		StatePath:       fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/state.json", driver),
		StatsPath:       fmt.Sprintf("/test-olake/drivers/%s/internal/testconfig/stats.json", driver),
	}
}

func IsRPSAboveBenchmark(config TestConfig, isBackfill bool) (bool, error) {
	benchmarkFile := utils.Ternary(isBackfill, "benchmark.json", "benchmark_cdc.json").(string)

	var stats map[string]interface{}
	if err := utils.UnmarshalFile(filepath.Join(config.HostRoot, fmt.Sprintf("drivers/%s/internal/testconfig/%s", config.Driver, "stats.json")), &stats, false); err != nil {
		return false, err
	}

	getRPSFromStats := func(stats map[string]interface{}) (float64, error) {
		rps, err := strconv.ParseFloat(strings.Split(stats["Speed"].(string), " ")[0], 64)
		if err != nil {
			return 0, err
		}
		return rps, nil
	}

	rps, err := getRPSFromStats(stats)
	if err != nil {
		return false, err
	}

	var benchmarkStats map[string]interface{}
	if err := utils.UnmarshalFile(filepath.Join(config.HostRoot, fmt.Sprintf("drivers/%s/internal/testconfig/%s", config.Driver, benchmarkFile)), &benchmarkStats, false); err != nil {
		return false, err
	}

	benchmarkRps, err := getRPSFromStats(benchmarkStats)
	if err != nil {
		return false, err
	}

	fmt.Printf("CurrentRPS: %.2f, BenchmarkRPS: %.2f\n", rps, benchmarkRps)

	if rps < 0*benchmarkRps {
		return false, fmt.Errorf("❌ RPS is less than benchmark RPS")
	}

	return true, nil
}

func InstallCmd() string {
	return "apt-get update && apt-get install -y openjdk-17-jre-headless maven default-mysql-client postgresql postgresql-client iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm jq && npm install -g chalk-cli"
}

func RunPerformanceTest(t *testing.T, config PerformanceTestConfig) {
	ctx := context.Background()

	discoverCommand := func(config TestConfig) string {
		return fmt.Sprintf("/test-olake/build.sh driver-%s discover --config %s", config.Driver, config.SourcePath)
	}

	syncCommand := func(config TestConfig, isBackfill bool) string {
		return fmt.Sprintf("/test-olake/build.sh driver-%s sync --config %s --catalog %s --destination %s %s", config.Driver, config.SourcePath, config.CatalogPath, config.DestinationPath, utils.Ternary(isBackfill, "", fmt.Sprintf("--state %s", config.StatePath)).(string))
	}

	updateStreamsCommand := func(config TestConfig, namespace string, streams ...string) string {
		if len(streams) == 0 {
			return ""
		}

		var conditions string
		for i, stream := range streams {
			if i > 0 {
				conditions += " or "
			}
			conditions += fmt.Sprintf(`.stream_name == "%s"`, stream)
		}

		jqExpr := fmt.Sprintf(
			`jq '.selected_streams["%s"] |= map(select(%s) | .normalization = true)' %s > /tmp/streams.json && mv /tmp/streams.json %s`,
			namespace,
			conditions,
			config.CatalogPath,
			config.CatalogPath,
		)

		return jqExpr
	}

	t.Run("performance", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", config.TestConfig.HostRoot),
				}
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			ConfigModifier: func(c *container.Config) {
				c.WorkingDir = "/test-olake"
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostReadies: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							_, output, err := utils.ExecContainerCmd(ctx, c, InstallCmd())
							require.NoError(t, err, fmt.Sprintf("Failed to install dependencies:\n%s", string(output)))

							conn, err := config.ConnectDB(ctx)
							require.NoError(t, err, "Failed to connect to database")
							defer config.CloseDB(conn)

							t.Run("backfill", func(t *testing.T) {
								discoverCmd := discoverCommand(*config.TestConfig)
								_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(output)))
								t.Log(string(output))

								updateStreamsCmd := updateStreamsCommand(*config.TestConfig, config.Namespace, config.BackfillStreams...)
								_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
								require.NoError(t, err, "Failed to update streams")

								syncCmd := syncCommand(*config.TestConfig, true)
								_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
								require.NoError(t, err, fmt.Sprintf("Failed to perform sync:\n%s", string(output)))
								t.Log(string(output))

								success, err := IsRPSAboveBenchmark(*config.TestConfig, true)
								require.NoError(t, err, "Failed to check RPS", err)
								require.True(t, success, fmt.Sprintf("%s backfill performance below benchmark", config.TestConfig.Driver))
								t.Logf("✅ SUCCESS: %s backfill", config.TestConfig.Driver)
							})

							if config.SupportsCDC {
								t.Run("cdc", func(t *testing.T) {
									err := config.SetupCDC(ctx, conn)
									require.NoError(t, err, "Failed to setup database for CDC")

									discoverCmd := discoverCommand(*config.TestConfig)
									_, output, err := utils.ExecContainerCmd(ctx, c, discoverCmd)
									require.NoError(t, err, fmt.Sprintf("Failed to perform discover:\n%s", string(output)))
									t.Log(string(output))

									updateStreamsCmd := updateStreamsCommand(*config.TestConfig, config.Namespace, config.CDCStreams...)
									_, _, err = utils.ExecContainerCmd(ctx, c, updateStreamsCmd)
									require.NoError(t, err, "Failed to update streams")

									syncCmd := syncCommand(*config.TestConfig, true)
									_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
									require.NoError(t, err, fmt.Sprintf("Failed to perform initial sync:\n%s", string(output)))
									t.Log(string(output))

									err = config.TriggerCDC(ctx, conn)
									require.NoError(t, err, "Failed to trigger CDC change")

									syncCmd = syncCommand(*config.TestConfig, false)
									_, output, err = utils.ExecContainerCmd(ctx, c, syncCmd)
									require.NoError(t, err, fmt.Sprintf("Failed to perform CDC sync:\n%s", string(output)))
									t.Log(string(output))

									success, err := IsRPSAboveBenchmark(*config.TestConfig, false)
									require.NoError(t, err, "Failed to check RPS", err)
									require.True(t, success, fmt.Sprintf("%s CDC performance below benchmark", config.TestConfig.Driver))
									t.Logf("✅ SUCCESS: %s cdc", config.TestConfig.Driver)
								})
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
		require.NoError(t, err, "Failed to start container")
		defer container.Terminate(ctx)
	})
}
