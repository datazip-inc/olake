package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMongoIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		Driver:             string(constants.MongoDB),
		ExpectedData:       ExpectedMongoData,
		ExpectedUpdateData: ExpectedUpdatedMongoData,
		DataTypeSchema:     MongoToIcebergSchema,
		ExecuteQuery:       ExecuteQuery,
	}
	testConfig.TestIntegration(t)
}
