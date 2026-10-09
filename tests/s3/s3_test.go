package s3

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// s3TestConfig builds the config every s3 suite shares for one source format variant. Each variant
// owns a testdata/<DataFormat>/ directory, which is what DataFormat selects.
func s3TestConfig(t *testing.T, variant S3TestVariant, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	config, err := testutils.NewTestConfig(t, constants.S3, "s3", nil,
		append([]testutils.TestConfigOption{testutils.WithDataFormat(variant.DataFormat)}, opts...)...)
	require.NoError(t, err, "failed to build the test config")
	config.ColumnToExclude = excludedColumn
	config.CursorField = S3CursorField
	config.PartitionRegex = S3PartitionRegex
	config.FilterConfig = variant.FilterConfig
	config.ExecuteQuery = ExecuteQueryFactory(variant, nil)
	return config
}

// s3BaseConfig is s3TestConfig with the integration suites' expected data.
func s3BaseConfig(t *testing.T, variant S3TestVariant, opts ...testutils.TestConfigOption) *integration.TestHandler {
	config := s3TestConfig(t, variant, opts...)
	cfg := &integration.TestHandler{
		TestConfig:                config,
		ExpectedData:              variant.ExpectedRowData(seedValues),
		ExpectedUpdatedData:       variant.ExpectedRowData(updatedValues),
		DestinationDataTypeSchema: variant.DestinationSchema,
		TypeMapping:               S3TypeMapping,
	}
	// The factory refreshes this handler's writer expectations, so it is rebuilt once the handler exists.
	config.ExecuteQuery = ExecuteQueryFactory(variant, cfg)
	return cfg
}

func TestS3Discover(t *testing.T) {
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			s3BaseConfig(t, variant).TestDiscover(t)
		})
	}
}

func TestS3Sync(t *testing.T) {
	t.Parallel()
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			t.Parallel()
			cfg := s3BaseConfig(t, variant)
			// The "evolve-schema" operation ships a file carrying a column discover has not
			// seen (see S3TestVariant.BuildEvolvedFile), so the update sync must land it in
			// the destination as a string column.
			cfg.UpdatedDestinationDataTypeSchema = variant.UpdatedDestinationSchema
			cfg.TestSync(t)
		})
	}
}

// TestS3Compatibility runs every source format. Each variant owns its testdata directory, source prefix
// and stream name, so the three share one destination namespace without colliding.
func TestS3Compatibility(t *testing.T) {
	t.Parallel()
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			t.Parallel()
			testHandler := &compatibility.TestHandler{
				DestinationSchema: variant.DestinationSchema,
			}
			testHandler.NewConfig = func(t *testing.T, version string) *testutils.TestConfig {
				return s3TestConfig(t, variant, testutils.WithDriverVersion(version))
			}
			testHandler.RunBackwardCompatibility(t)
		})
	}
}
