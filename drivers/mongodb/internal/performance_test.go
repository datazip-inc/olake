package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMongodbPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:     testutils.GetTestConfig("mongodb"),
		Namespace:      "test",
		BackfillStream: "users",
		CDCStream:      "users_cdc",
		ExecuteQuery:   ExecuteQueryPerformance,
		SupportsCDC:    true,
	}

	config.TestPerformance(t)
}
