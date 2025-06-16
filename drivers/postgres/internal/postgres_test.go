package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
)

// Test functions using base utilities
func TestPostgresSetup(t *testing.T) {
	_, absDriver := testPostgresClient(t)
	absDriver.TestSetup(t)
}

func TestPostgresDiscover(t *testing.T) {
	client, absDriver := testPostgresClient(t)
	absDriver.TestDiscover(t, client, ExecuteQuery)
}

func TestPostgresRead(t *testing.T) {
	client, absDriver := testPostgresClient(t)
	absDriver.TestRead(t, client, ExecuteQuery, abstract.PostgresSchema)
}
