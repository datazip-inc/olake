package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMongodbPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:          testutils.GetTestConfig("mongodb"),
		Namespace:           "test",
		BackfillStreams:     []string{"users"},
		CDCStreams:          []string{"users_cdc"},
		ExecuteQuery:        ExecuteQueryPerformance,
		SupportsCDC:         true,
		UsesPreChunkedState: false,
	}

	config.TestPerformance(t)
}
