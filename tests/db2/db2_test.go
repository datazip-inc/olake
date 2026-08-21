package db2

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
)

// db2BaseConfig returns an IntegrationTest pre-populated with all fields shared
func db2BaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                (&testutils.TestConfig{Driver: string(constants.DB2)}).WithImagePlatform("linux/amd64"),
		ExpectedData:              ExpectedDB2Data,
		DestinationDataTypeSchema: DB2ToDestinationSchema,
	}
	cfg.TestConfig.Namespace = "DB2INST1"
	cfg.TestConfig.ExecuteQuery = ExecuteQuery
	cfg.TestConfig.DestinationDB = "db2_testdb_db2inst1"
	cfg.TestConfig.CursorField = "COL_CURSOR:COL_TIMESTAMP"
	cfg.TestConfig.PartitionRegex = "/{id, identity}"
	cfg.TestConfig.ColumnToExclude = "EXCLUDEDCOLUMN"
	cfg.TestConfig.FilterConfig = `{
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
	return cfg
}

func TestDB2Discover(t *testing.T) {
	db2BaseConfig().TestDiscover(t)
}

func TestDB2Sync(t *testing.T) {
	t.Parallel()
	cfg := db2BaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedDB2Data
	cfg.UpdatedDestinationDataTypeSchema = UpdatedDB2ToDestinationSchema
	cfg.TestSync(t)
}

func TestDB22PC(t *testing.T) {
	t.Parallel()
	db2BaseConfig().Test2PCIntegration(t)
}
