package mysql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// mysqlTestConfig builds the config every mysql suite shares: the source, the namespace and the stream settings.
func mysqlTestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.MySQL, "olake_mysql_test", ExecuteQuery, opts...)
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "id_cursor:id_smallint"
	cfg.PartitionRegex = "/{id,identity}"
	cfg.ColumnToExclude = "excludedColumn"
	cfg.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "price_double",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "created_timestamp",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`

	return cfg
}

// mysqlBaseConfig is mysqlTestConfig with the integration suites' expected data.
func mysqlBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                mysqlTestConfig(t, opts...),
		ExpectedData:              ExpectedMySQLData,
		DestinationDataTypeSchema: MySQLToDestinationSchema,
		TypeMapping:               MySQLTypeMapping,
		DefaultCDCColumnsSchema:   ExpectedMySQLDefaultCDCColumnsSchema,
	}
}

func TestMySQLDiscover(t *testing.T) {
	mysqlBaseConfig(t).TestDiscover(t)
}

func TestMySQLSync(t *testing.T) {
	t.Parallel()
	cfg := mysqlBaseConfig(t)
	cfg.CursorField = "id_cursor_binary:id_smallint"
	cfg.ExpectedUpdatedData = ExpectedUpdatedData()
	cfg.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
	cfg.TestSync(t)
}

// TestMySQLBinarySync keys, partitions and filters on binary columns. The filter cases use the binary cursor so
// incremental reads the filtered insert row too; their filter values are hex.
func TestMySQLBinarySync(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		configure func(cfg *integration.TestHandler)
	}{
		{
			// a binary primary key and partition column
			name: "partition",
			configure: func(cfg *integration.TestHandler) {
				cfg.PrimaryKey = "id_cursor_binary"
				cfg.PartitionRegex = "/{data_fixed_binary,identity}"
				cfg.ExpectedUpdatedData["_olake_id"] = binaryCursorOlakeID(1)
			},
		},
		{
			// keeps only the filtered rows (X'00' padded to BINARY(16)) and the updated row the update phase
			// asserts, whose X'FFFE' is not valid UTF-8
			name: "filter_inclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "id_cursor_binary:id_smallint"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "data_fixed_binary",
                            "operator": "=",
                            "value": "00000000000000000000000000000000"
                        },
                        {
                            "column": "data_fixed_binary",
                            "operator": "=",
                            "value": "fffe0000000000000000000000000000"
                        }
                    ]
                }`
				cfg.ExpectedData = ExpectedFilteredData
			},
		},
		{
			// drops the filtered rows; they come through if "!=" breaks or the unpadded X'00' matches
			name: "filter_exclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "id_cursor_binary:id_smallint"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "data_varbinary",
                            "operator": "!=",
                            "value": "01"
                        },
                        {
                            "column": "data_fixed_binary",
                            "operator": "=",
                            "value": "00"
                        }
                    ]
                }`
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := mysqlBaseConfig(t)
			cfg.ExpectedUpdatedData = ExpectedUpdatedData()
			cfg.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
			tc.configure(cfg)
			cfg.TestCommonSync(t)
		})
	}
}

func TestMySQL2PC(t *testing.T) {
	t.Parallel()
	mysqlBaseConfig(t).Test2PCIntegration(t)
}

func TestMySQLIcebergDV(t *testing.T) {
	t.Parallel()
	mysqlBaseConfig(t).TestIcebergDV(t)
}

func TestMySQLPerformance(t *testing.T) {
	cfg, err := testutils.NewTestConfig(t, constants.MySQL, "benchmark", ExecuteQuery)
	require.NoError(t, err, "failed to build the test config")

	perf := &performance.TestHandler{
		TestConfig:      cfg,
		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
	}

	perf.TestPerformance(t)
}

// TestMySQLCompatibility pins the backward-compatibility contract for the driver that owns three of the
// six version gates -- the binlog timestamp location (v2), the timezone offset (v3) and the
// UNSIGNED widening (v4), see constants/state_version.go.
func TestMySQLCompatibility(t *testing.T) {
	t.Parallel()
	testHandler := &compatibility.TestHandler{
		DestinationSchema: MySQLToDestinationSchema,
		CDCColumnsSchema:  ExpectedMySQLDefaultCDCColumnsSchema,
		ColumnTypes:       seedColumnTypes(),
	}
	testHandler.NewConfig = func(t *testing.T, version string) *testutils.TestConfig {
		return mysqlTestConfig(t, testutils.WithDriverVersion(version))
	}
	testHandler.RunBackwardCompatibility(t)
}
