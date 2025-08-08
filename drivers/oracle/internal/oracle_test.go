package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestOraclePerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:          testutils.GetTestConfig(string(constants.Oracle)),
		Namespace:           "ADMIN",
		BackfillStreams:     []string{"USERS"},
		CDCStreams:          []string{""},
		ExecuteQuery:        nil,
		SupportsCDC:         false,
		UsesPreChunkedState: false,
	}

	config.TestPerformance(t)
}
