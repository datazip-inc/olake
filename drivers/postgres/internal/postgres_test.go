package driver

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// import (
// 	"testing"

// 	"github.com/datazip-inc/olake/drivers/abstract"
// )

// // Test functions using base utilities
// func TestPostgresSetup(t *testing.T) {
// 	_, absDriver := testPostgresClient(t)
// 	absDriver.TestSetup(t)
// }

// func TestPostgresDiscover(t *testing.T) {
// 	client, absDriver := testPostgresClient(t)
// 	absDriver.TestDiscover(t, client, ExecuteQuery)
// }

// func TestPostgresRead(t *testing.T) {
// 	client, absDriver := testPostgresClient(t)
// 	absDriver.TestRead(t, client, ExecuteQuery, abstract.PostgresSchema)
// }

const (
	testTablePrefix       = "test_table_olake"
	sparkConnectAddress   = "sc://spark-iceberg:15002"
	icebergDatabase       = "olake_iceberg"
	cdcInitializationWait = 2 * time.Second
	cdcProcessingWait     = 60 * time.Second
)

var currentTestTable = fmt.Sprintf("%s_%d", testTablePrefix, time.Now().Unix())

// func setupPostgresClient(t *testing.T) *sqlx.DB {
// 	t.Helper()

// 	config := Config{
// 		Host:     defaultPostgresHost,
// 		Port:     defaultPostgresPort,
// 		Username: defaultPostgresUser,
// 		Password: defaultPostgresPassword,
// 		Database: defaultPostgresDB,
// 		SSLConfiguration: &utils.SSLConfig{
// 			Mode: "disable",
// 		},
// 		BatchSize: defaultBatchSize,
// 	}

// 	pgDriver := &Postgres{
// 		config: &config,
// 	}
// 	pgDriver.CDCSupport = true
// 	pgDriver.cdcConfig = CDC{
// 		InitialWaitTime: defaultCDCWaitTime,
// 		ReplicationSlot: defaultReplicationSlot,
// 	}

// 	err := pgDriver.Setup(context.Background())
// 	require.NoError(t, err, "Failed to setup PostgreSQL driver")
// 	return pgDriver.client
// }

func TestPostgresIntegration(t *testing.T) {
	ctx := context.Background()

	cwd, err := os.Getwd()
	require.NoErrorf(t, err, "Failed to get current working directory")
	projectRoot := filepath.Join(cwd, "../../../..")
	testdataDir := filepath.Join(projectRoot, "drivers/postgres/internal/testdata")
	sourceConfig := filepath.Join("drivers/postgres/internal/testdata", "source.json")
	streamsFile := filepath.Join("drivers/postgres/internal/testdata", "streams.json")

	t.Run("Discover", func(t *testing.T) {
		req := testcontainers.ContainerRequest{
			Image: "golang:1.23.2",
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = []string{
					fmt.Sprintf("%s:/olake:ro", projectRoot),
					fmt.Sprintf("%s:/olake/drivers/postgres/internal/testdata:rw", testdataDir),
				}
			},
			ConfigModifier: func(config *container.Config) {
				config.WorkingDir = "/olake"
			},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
				{
					PostStarts: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							installCmd := "apt-get update && apt-get install -y postgresql-client"
							code, out, err := c.Exec(ctx, []string{"/bin/sh", "-c", installCmd})
							if err != nil {
								return fmt.Errorf("failed to install postgresql-client: %w\n%s", err, out)
							}
							if code != 0 {
								return fmt.Errorf("install postgresql-client exited with code %d:\n%s", code, out)
							}

							// buildCmd := "go build -o /olake/drivers/postgres/main.go"
							// code, out, err = c.Exec(ctx, []string{"/bin/sh", "-c", buildCmd})
							// if err != nil {
							// 	outStr, _ := io.ReadAll(out)
							// 	return fmt.Errorf("failed to build driver: %w\n%s", err, string(outStr))
							// }
							// if code != 0 {
							// 	outStr, _ := io.ReadAll(out)
							// 	return fmt.Errorf("build exited with code %d:\n%s", code, string(outStr))
							// }
							// t.Logf("psql installed")
							setupSQL := fmt.Sprintf(`
								CREATE TABLE IF NOT EXISTS %s (
									col_bigint BIGINT,
									col_bigserial BIGSERIAL PRIMARY KEY,
									col_bool BOOLEAN,
									col_char CHAR(1),
									col_character CHAR(10),
									col_character_varying VARCHAR(50),
									col_date DATE,
									col_daterange DATERANGE,
									col_decimal NUMERIC,
									col_double_precision DOUBLE PRECISION,
									col_float4 REAL,
									col_int INT,
									col_int2 SMALLINT,
									col_integer INTEGER,
									col_interval INTERVAL,
									col_json JSON,
									col_jsonb JSONB,
									col_jsonpath JSONPATH,
									col_name NAME,
									col_numeric NUMERIC,
									col_real REAL,
									col_text TEXT,
									col_time TIME,
									col_timestamp TIMESTAMP,
									col_timestamptz TIMESTAMPTZ,
									col_timetz TIMETZ,
									col_uuid UUID,
									col_varbit VARBIT(20),
									col_xml XML,
									CONSTRAINT unique_custom_key_7 UNIQUE (col_bigserial)
								);
								TRUNCATE %s;
								INSERT INTO %s (
									col_bigint, col_bool, col_char, col_character,
									col_character_varying, col_date, col_daterange, col_decimal,
									col_double_precision, col_float4, col_int, col_int2, col_integer,
									col_interval, col_json, col_jsonb, col_jsonpath, col_name, col_numeric,
									col_real, col_text, col_time, col_timestamp, col_timestamptz, col_timetz,
									col_uuid, col_varbit, col_xml
								) VALUES (
									1234567890123456789, TRUE, 'c', 'char_val',
									'varchar_val', '2023-01-01', '[2023-01-01,2023-01-02)', 123.45,
									123.456789, 123.45, 123, 123, 12345, '1 hour', '{"key": "value"}',
									'{"key": "value"}', '$.key', 'test_name', 123.45, 123.45,
									'sample text', '12:00:00', '2023-01-01 12:00:00',
									'2023-01-01 12:00:00+00', '12:00:00+00',
									'123e4567-e89b-12d3-a456-426614174000', B'101010',
									'<tag>value</tag>'
								);
								SELECT * FROM %s;
							`, currentTestTable, currentTestTable, currentTestTable, currentTestTable)
							code, out, err = c.Exec(ctx, []string{
								"psql", "-U", "postgres", "-h", "host.docker.internal", "-p", "5433", "-c", setupSQL,
							})
							if err != nil {
								return fmt.Errorf("pre-start psql exec error: %w\n%s", err, out)
							}

							var data []byte
							_, err = out.Read(data)
							if code != 0 {
								return fmt.Errorf("pre-start psql exit %d:\n%s, data being read: %s", code, err, data)
							}
							return nil
						},
					},
					PreStops: []testcontainers.ContainerHook{
						func(ctx context.Context, c testcontainers.Container) error {
							cmd := fmt.Sprintf(
								"/build.sh driver-postgres discover --config %s > %s",
								sourceConfig, streamsFile,
							)
							code, out, err := c.Exec(ctx, []string{"/bin/sh", "-c", cmd})
							if err != nil {
								outStr, _ := io.ReadAll(out)
								return fmt.Errorf("pre-stop run discover error: %w\n%s", err, string(outStr))
							}
							outStr, readErr := io.ReadAll(out)
							if readErr != nil {
								return fmt.Errorf("failed to read discover command output: %w", readErr)
							}
							t.Logf("---- Discover command: %s ----\n", cmd)
							t.Logf("---- Discover output ----\n%s\n----------------------------", string(outStr))
							if code != 0 {
								return fmt.Errorf("pre-stop discover exit %d\n%s", code, string(outStr))
							}
							return nil
						},
						func(ctx context.Context, c testcontainers.Container) error {
							code, out, err := c.Exec(ctx, []string{"cat", streamsFile})
							if err != nil {
								return fmt.Errorf("pre-stop cat streams.json error: %w", err)
							}
							if code != 0 {
								return fmt.Errorf("pre-stop cat exit %d", code)
							}
							outStr, err := io.ReadAll(out)
							if err != nil {
								return fmt.Errorf("failed to read output: %w", err)
							}
							t.Logf("---- contents of %s ----\n%s\n----------------------------", streamsFile, string(outStr))
							t.Logf(currentTestTable)
							t.Logf("stream contains the test table: %v", strings.Contains(string(outStr), currentTestTable))
							if !strings.Contains(string(outStr), currentTestTable) {
								return fmt.Errorf("did not find %q in streams.json:\n%s", currentTestTable, out)
							}
							return nil
						},
						func(ctx context.Context, c testcontainers.Container) error {
							dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s;", currentTestTable)
							code, out, err := c.Exec(ctx, []string{
								"psql", "-U", "postgres", "-h", "host.docker.internal", "-p", "5433", "-c", dropSQL,
							})
							if err != nil {
								return fmt.Errorf("pre-stop drop table error: %w\n%s", err, out)
							}
							if code != 0 {
								return fmt.Errorf("pre-stop drop table exit %d:\n%s", code, out)
							}
							return nil
						},
					},
				},
			},
			Cmd:      []string{"sleep", "300"},
			Networks: []string{"iceberg_net"},
		}

		discoverContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err, "Failed to start discover container")
		defer discoverContainer.Terminate(ctx)
	})
}
