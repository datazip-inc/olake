package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMySQLIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		Driver:             string(constants.MySQL),
		ExpectedData:       ExpectedMySQLData,
		ExpectedUpdateData: ExpectedUpdatedMySQLData,
		DataTypeSchema:     MySQLSchema,
		ExecuteQuery:       ExecuteQuery,
	}
	testConfig.TestIntegration(t)
}
