package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils/testutils"
	_ "github.com/lib/pq"
)

func TestPostgresPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:     testutils.GetTestConfig("postgres"),
		Namespace:      "public",
		BackfillStream: "test",
		CDCStream:      "test_cdc",
		ExecuteQuery:   ExecuteQueryPerformance,
		SupportsCDC:    true,
	}

	config.TestPerformance(t)
}
