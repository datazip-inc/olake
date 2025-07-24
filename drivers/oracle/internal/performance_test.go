package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils/testutils"
)

func TestOraclePerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:     testutils.GetTestConfig("oracle"),
		Namespace:      "ADMIN",
		BackfillStream: "USERS",
		CDCStream:      "",
		ExecuteQuery:   nil,
		SupportsCDC:    false,
	}

	config.TestPerformance(t)
}
