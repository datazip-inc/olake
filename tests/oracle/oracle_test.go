package oracle

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
)

// oracleBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the oracle suites.
func oracleBaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                &testutils.TestConfig{Driver: string(constants.Oracle)},
		ExpectedData:              ExpectedOracleData,
		DestinationDataTypeSchema: OracleToDestinationSchema,
	}
	cfg.TestConfig.Namespace = "MYUSER"
	cfg.TestConfig.ExecuteQuery = ExecuteQuery
	cfg.TestConfig.DestinationDB = "oracle_myuser"
	cfg.TestConfig.CursorField = "COL_CURSOR:COL_SMALLINT"
	cfg.TestConfig.PartitionRegex = "/{id, identity}"
	cfg.TestConfig.ColumnToExclude = "EXCLUDEDCOLUMN"
	cfg.TestConfig.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "COL_DOUBLE_PRECISION",
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

func TestOracleDiscover(t *testing.T) {
	oracleBaseConfig().TestDiscover(t)
}

func TestOracleSync(t *testing.T) {
	t.Parallel()
	cfg := oracleBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedOracleData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedOracleToDestinationSchema
	cfg.TestSync(t)
}

func TestOracle2PC(t *testing.T) {
	t.Parallel()
	oracleBaseConfig().Test2PCIntegration(t)
}
