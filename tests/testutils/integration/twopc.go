package integration

import (
	"fmt"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// Test2PCIntegration runs the full Two-Phase Commit (2PC) failure-recovery integration test
// suite against the driver image. It exercises CDC and incremental state-recovery scenarios
// independently of the happy-path integration tests, allowing them to be scheduled and
// reported separately.
func (cfg *Test) Test2PCIntegration(t *testing.T) {
	if cfg.Suite == "" {
		cfg.Suite = "2pc"
	} else {
		cfg.Suite += "_2pc"
	}
	cfg.IsolateSource = true
	cfg.Validate(t)
	cfg.Setup(t)

	ctx := t.Context()

	currentTestTable := cfg.GetTableName()

	t.Run("Sync", func(t *testing.T) {
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "add")

		if err := testutils.UpdateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
			t.Fatalf("failed to enable normalization and partition regex in streams.json: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", "test_stream.json")

		writerTypes := []struct {
			name     string
			useArrow bool
		}{
			{"Legacy", false},
			{"Arrow", true},
		}

		if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(cfg.TestConfig.Driver)) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC CDC Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.IcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.Iceberg2PCCDCRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC CDC Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		if cfg.TestConfig.Driver != string(constants.Kafka) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC Incremental Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.IcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.Iceberg2PCIncrementalRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC Incremental Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		if testutils.KeepTestData() {
			t.Logf("keeping %s 2PC sync test data (%s) is set", cfg.TestConfig.Driver, testutils.KeepTestDataEnvVar)
		} else {
			cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
			t.Logf("%s 2PC sync test cleanup", cfg.TestConfig.Driver)
		}
	})
}
