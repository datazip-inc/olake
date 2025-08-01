package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils/testutils"
)

func TestOraclePerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:          testutils.GetTestConfig("oracle"),
		Namespace:           "ADMIN",
		BackfillStreams:     []string{"USERS"},
		CDCStreams:          []string{""},
		ExecuteQuery:        nil,
		SupportsCDC:         false,
		UsesPreChunkedState: false,
	}

	config.TestPerformance(t)
}
