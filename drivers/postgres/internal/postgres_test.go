package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/docker/docker/api/types/container"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	currentTestTable      = "postgres_test_table_olake"
	sourceConfigPath      = "/test-olake/drivers/postgres/internal/testdata/source.json"
	streamsFilePath       = "/test-olake/drivers/postgres/internal/testdata/streams.json"
	destinationConfigPath = "/test-olake/drivers/postgres/internal/testdata/destination.json"
	installCmd            = "apt-get update && apt-get install -y openjdk-17-jre-headless postgresql-client iproute2 dnsutils iputils-ping netcat-openbsd nodejs npm && npm install -g chalk-cli"
)

func sortJSONString(jsonStr string) (string, error) {
	start := strings.IndexRune(jsonStr, '{')
	end := strings.LastIndex(jsonStr, "}")
	if start == -1 || end == -1 || start > end {
		return "", fmt.Errorf("no valid JSON object found")
	}

	// 2) Slice out exactly from the first '{' to the last '}'
	core := jsonStr[start : end+1]

	var data interface{}
	if err := json.Unmarshal([]byte(core), &data); err != nil {
		return "", fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Convert to compact JSON string (no formatting)
	compactBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Convert to string and sort characters lexicographically
	jsonString := string(compactBytes)
	chars := []rune(jsonString)
	sort.Slice(chars, func(i, j int) bool {
		return chars[i] < chars[j]
	})

	return string(chars), nil
}

func TestPostgresIntegration(t *testing.T) {
	ctx := context.Background()

	cwd, err := os.Getwd()
	t.Logf("Host working directory: %s", cwd)
	require.NoErrorf(t, err, "Failed to get current working directory")
	projectRoot := filepath.Join(cwd, "../../..")
	t.Logf("Root Project directory: %s", projectRoot)
	testdataDir := filepath.Join(projectRoot, "drivers/postgres/internal/testdata")
	dummyStreamFilePath := filepath.Join(testdataDir, "test_streams.json")

	t.Run("Discover", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", projectRoot),
					fmt.Sprintf("%s:/test-olake/drivers/postgres/internal/testdata:rw", testdataDir),
				}
			},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/test-olake"
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostStarts: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							// 1. Install required tools
							if code, out, err := execCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %w\n%s", code, err, out)
							}

							// Check working directory
							// code, out, err := c.Exec(ctx, []string{"pwd"})
							// if err != nil {
							// 	t.Logf("Failed to get container working directory: %v", err)
							// } else {
							// 	outStr, _ := io.ReadAll(out)
							// 	t.Logf("Container Working Directory: %s", strings.TrimSpace(string(outStr)))
							// }

							// 2. Create test table and insert data
							db, err := sqlx.ConnectContext(ctx, "postgres",
								"postgres://postgres@localhost:5433/postgres?sslmode=disable",
							)
							require.NoError(t, err, "failed to connect to postgres")
							defer db.Close()
							ExecuteQuery(ctx, t, db, currentTestTable, "create")
							ExecuteQuery(ctx, t, db, currentTestTable, "clean")
							ExecuteQuery(ctx, t, db, currentTestTable, "add")

							// 3. Run discover command
							discoverCmd := fmt.Sprintf("/test-olake/build.sh driver-postgres discover --config %s", sourceConfigPath)
							if code, out, err := execCommand(ctx, c, discoverCmd); err != nil || code != 0 {
								return fmt.Errorf("discover failed (%d): %w\n%s", code, err, string(out))
							}

							streamsJSON, err := os.ReadFile(dummyStreamFilePath)
							if err != nil {
								return fmt.Errorf("failed to read expected streams JSON: %w", err)
							}
							testStreamsCmd := fmt.Sprintf("cat %s", streamsFilePath)
							_, testStreamJSON, err := execCommand(ctx, c, testStreamsCmd)
							if err != nil {
								return fmt.Errorf("failed to read actual streams JSON: %w", err)
							}
							t.Logf("testStreamJson: %s", strings.TrimSpace(string(streamsJSON)))
							expectedStream, err := sortJSONString(strings.TrimSpace(string(streamsJSON)))
							if err != nil {
								return fmt.Errorf("failed to sort expected JSON as string: %w", err)
							}
							t.Logf("testStreamJson: %s", strings.TrimSpace(string(testStreamJSON)))
							testStream, err := sortJSONString(strings.TrimSpace(string(testStreamJSON)))
							if err != nil {
								return fmt.Errorf("failed to sort actual JSON as string: %w", err)
							}

							if expectedStream != testStream {
								return fmt.Errorf("streams.json does not match expected test_streams.json\nExpected:\n%s\nGot:\n%s", expectedStream, testStream)
							}
							return nil
						},
					},
					PreStops: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							cleanupSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s;", currentTestTable)
							psqlCmd := fmt.Sprintf(
								"psql -U postgres -h host.docker.internal -p 5433 -c %q",
								cleanupSQL,
							)
							_, _, _ = execCommand(ctx, c, psqlCmd) // Best-effort cleanup
							return nil
						},
					},
				},
			},
			Cmd: []string{"sleep", "300"},
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err, "Container startup failed")
		defer container.Terminate(ctx)
	})

	t.Run("Sync", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/test-olake:rw", projectRoot),
					fmt.Sprintf("%s:/test-olake/drivers/postgres/internal/testdata:rw", testdataDir),
				}
			},
			Networks: []string{"local-test_iceberg_net"},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/test-olake"
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostStarts: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							if code, out, err := execCommand(ctx, c, installCmd); err != nil || code != 0 {
								return fmt.Errorf("install failed (%d): %w\n%s", code, err, out)
							}

							db, err := sqlx.ConnectContext(ctx, "postgres",
								"postgres://postgres@localhost:5433/postgres?sslmode=disable",
							)
							require.NoError(t, err, "failed to connect to postgres")
							defer db.Close()
							ExecuteQuery(ctx, t, db, currentTestTable, "create")
							ExecuteQuery(ctx, t, db, currentTestTable, "clean")
							ExecuteQuery(ctx, t, db, currentTestTable, "add")

							sparkConnectHost := "host.docker.internal"

							// Spark network diagnostics
							diagCmd := fmt.Sprintf(`echo "Testing network connectivity to %s:15002";ping -c 1 %s;nc -zv %s 15002;`, sparkConnectHost, sparkConnectHost, sparkConnectHost)

							if code, out, err := execCommand(ctx, c, diagCmd); err != nil || code != 0 {
								t.Logf("Network diagnostics failed (%d):\n%s", code, out)
							}

							fullSyncCmd := fmt.Sprintf("/test-olake/build.sh driver-postgres sync --config %s --catalog %s --destination %s", sourceConfigPath, streamsFilePath, destinationConfigPath)
							if code, out, err := execCommand(ctx, c, fullSyncCmd); err != nil || code != 0 {
								return fmt.Errorf("Full refresh failed (%d): %w\n%s", code, err, string(out))
							}

							abstract.VerifyIcebergSync(t, currentTestTable, "5", sparkConnectHost, abstract.PostgresSchema)
							return nil
						},
					},
					PreStops: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							cleanupSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s;", currentTestTable)
							psqlCmd := fmt.Sprintf(
								"psql -U postgres -h host.docker.internal -p 5433 -c %q",
								cleanupSQL,
							)
							_, _, _ = execCommand(ctx, c, psqlCmd) // Best-effort cleanup
							return nil
						},
					},
				},
			},
			Cmd: []string{"sleep", "300"},
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err, "Container startup failed")
		defer container.Terminate(ctx)
	})

}

// Helper function to execute container commands
func execCommand(
	ctx context.Context,
	c testcontainers.Container,
	cmd string,
) (int, []byte, error) {
	code, reader, err := c.Exec(ctx, []string{"/bin/sh", "-c", cmd})
	if err != nil {
		return code, nil, err
	}
	output, _ := io.ReadAll(reader)
	return code, output, nil
}
