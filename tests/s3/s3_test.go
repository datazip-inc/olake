package s3

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// s3BaseConfig returns an IntegrationTest for one source format variant. Each variant owns a
// testdata/<DataFormat>/ directory, which is what DataFormat selects.
func s3BaseConfig(t *testing.T, variant S3TestVariant) *integration.Test {
	config, err := testutils.NewTestConfig(t, constants.S3, "s3", S3DestinationDB, nil,
		testutils.WithDataFormat(variant.DataFormat))
	require.NoError(t, err, "failed to build the test config")
	config.ColumnToExclude = excludedColumn
	config.CursorField = S3CursorField
	config.PartitionRegex = S3PartitionRegex
	config.FilterConfig = variant.FilterConfig

	cfg := &integration.Test{
		TestConfig:                config,
		ExpectedData:              variant.ExpectedRowData(seedValues),
		ExpectedUpdatedData:       variant.ExpectedRowData(updatedValues),
		DestinationDataTypeSchema: variant.DestinationSchema,
	}
	// The factory closes over the test it drives, so it is built once that exists.
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
// func TestS3Compatibility(t *testing.T) {
// 	t.Parallel()
// 	for _, variant := range S3TestVariants {
// 		t.Run(variant.Name, func(t *testing.T) {
// 			t.Parallel()
// 			compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
// 				base := s3BaseConfig(t, variant)
// 				cfg := &compatibility.Test{IntegrationTest: base}
// 				// Same isolation TestS3Sync applies: Parquet and ParquetInMemory share a
// 				// DataFormat, so without it they share every name the suite derives from it.
// 				cfg.IntegrationTest.Suite = variant.Name
// 				// Type tags for compatibility_rules.json's s3 rules; the driver-level _olake_id and
// 				// _last_modified_time policies are column-keyed there and need no tags.
// 				switch variant.DataFormat {
// 				case "json":
// 					cfg.ColumnTypes = map[string][]string{"mixed_col": {"mixed"}}
// 				case "csv":
// 					cfg.ColumnTypes = map[string][]string{evolvedColumn: {"evolved"}}
// 				case "parquet":
// 					cfg.ColumnTypes = map[string][]string{
// 						"map_col":    {"map"},
// 						"struct_col": {"struct"},
// 						"list_col":   {"list"},
// 						"int96_col":  {"int96"},
// 						"ts_col":     {"timestamp"},
// 						"ts_ms_col":  {"timestamp"},
// 						"ts_ns_col":  {"timestamp"},
// 						"ts_far_col": {"timestamp"},
// 						"uuid_col":   {"uuid"},
// 					}
// 				}
// 				// The closure reads SeedExcludedColumns at call time; RunBackwardCompatibility fills it
// 				// in after resolving the rules above against the baseline.
// 				cfg.SupportsSeedExclusion = true
// 				base.TestConfig.ExecuteQuery = ExecuteQueryFactoryExcluding(variant, cfg.IntegrationTest, func() []string { return cfg.SeedExcludedColumns })
// 				return cfg
// 			})
// 		})
// 	}
// }
