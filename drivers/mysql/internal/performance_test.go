package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMySQLPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:     testutils.GetTestConfig("mysql"),
		Namespace:      "mysql",
		BackfillStream: "test",
		CDCStream:      "test_cdc",
		ExecuteQuery:   ExecuteQueryPerformance,
		SupportsCDC:    true,
	}

	config.TestPerformance(t)
}
