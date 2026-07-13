package oracle

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// oracleBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between the oracle integration tests.
func oracleBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig("oracle"),
		Namespace:                 "MYUSER",
		ExpectedData:              ExpectedOracleData,
		DestinationDataTypeSchema: OracleToDestinationSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "oracle_myuser",
		CursorField:               "COL_CURSOR:COL_SMALLINT",
		PartitionRegex:            "/{id, identity}",
		ColumnToExclude:           "EXCLUDEDCOLUMN",
		FilterConfig: `{
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
                }`,
	}
}

func TestDiscover(t *testing.T) {
	oracleBaseConfig().TestDiscover(t)
}

func TestSync(t *testing.T) {
	cfg := oracleBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedOracleData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedOracleToDestinationSchema
	cfg.TestSync(t)
}

func Test2PC(t *testing.T) {
	oracleBaseConfig().Test2PCIntegration(t)
}
