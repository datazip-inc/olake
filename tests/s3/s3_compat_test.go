package s3

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestS3Compatibility runs every source format. Each variant owns its testdata directory, source prefix
// and stream name, so the three share one destination namespace without colliding.
func TestS3Compatibility(t *testing.T) {
	t.Parallel()
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			t.Parallel()
			compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
				cfg := &compatibility.Test{IntegrationTest: s3BaseConfig(variant)}
				// Same isolation TestS3Sync applies: Parquet and ParquetInMemory share a
				// DataFormat, so without it they share every name the suite derives from it.
				cfg.IntegrationTest.Suite = variant.Name
				// Type tags for compatibility_rules.json's s3 rules; the driver-level _olake_id and
				// _last_modified_time policies are column-keyed there and need no tags.
				switch variant.DataFormat {
				case "json":
					cfg.ColumnTypes = map[string][]string{"mixed_col": {"mixed"}}
				case "csv":
					cfg.ColumnTypes = map[string][]string{evolvedColumn: {"evolved"}}
				case "parquet":
					cfg.ColumnTypes = map[string][]string{
						"map_col":    {"map"},
						"struct_col": {"struct"},
						"list_col":   {"list"},
						"int96_col":  {"int96"},
						"ts_col":     {"timestamp"},
						"ts_ms_col":  {"timestamp"},
						"ts_ns_col":  {"timestamp"},
						"ts_far_col": {"timestamp"},
						"uuid_col":   {"uuid"},
					}
				}
				// The closure reads SeedExcludedColumns at call time; RunBackwardCompatibility fills it
				// in after resolving the rules above against the baseline.
				cfg.SupportsSeedExclusion = true
				cfg.IntegrationTest.TestConfig.ExecuteQuery = ExecuteQueryFactoryExcluding(variant, cfg.IntegrationTest, func() []string { return cfg.SeedExcludedColumns })
				return cfg
			})
		})
	}
}
