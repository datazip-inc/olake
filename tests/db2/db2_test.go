package db2

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// db2BaseConfig returns an IntegrationTest pre-populated with all fields shared
func db2BaseConfig(t *testing.T) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.DB2, "DB2INST1", "db2_testdb_db2inst1", ExecuteQuery,
		testutils.WithImagePlatform("linux/amd64"))
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "COL_CURSOR:COL_TIMESTAMP"
	cfg.PartitionRegex = "/{id, identity}"
	cfg.ColumnToExclude = "EXCLUDEDCOLUMN"
	cfg.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "COL_DOUBLE",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "COL_TIMESTAMP",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`

	return &integration.Test{
		TestConfig:                cfg,
		ExpectedData:              ExpectedDB2Data,
		DestinationDataTypeSchema: DB2ToDestinationSchema,
	}
}

func TestDB2Discover(t *testing.T) {
	db2BaseConfig(t).TestDiscover(t)
}

func TestDB2Sync(t *testing.T) {
	t.Parallel()
	cfg := db2BaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedDB2Data
	cfg.UpdatedDestinationDataTypeSchema = UpdatedDB2ToDestinationSchema
	cfg.TestSync(t)
}

func TestDB22PC(t *testing.T) {
	t.Parallel()
	db2BaseConfig(t).Test2PCIntegration(t)
}

// TestDB2Compatibility pins the backward-compatibility contract: the same scenarios run on a released
// baseline image and on this build after the initial load, and the destinations must match.
// See tests/testutils/compatibility.go.
// func TestDB2Compatibility(t *testing.T) {
// 	t.Parallel()
// 	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
// 		base := db2BaseConfig(t)
// 		base.ExpectedUpdatedData = ExpectedUpdatedDB2Data
// 		base.UpdatedDestinationDataTypeSchema = UpdatedDB2ToDestinationSchema
// 		cfg := &compatibility.Test{IntegrationTest: base}
// 		// Type tags for compatibility_rules.json's db2 rules; floor and descriptions live there too.
// 		cfg.ColumnTypes = map[string][]string{"col_decfloat": {"decfloat"}}
// 		return cfg
// 	})
// }
