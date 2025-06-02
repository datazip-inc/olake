package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
)

// Test functions using base utilities
func TestPostgresSetup(t *testing.T) {
	client, _, pClient := testPostgresClient(t)
	base.TestSetup(t, pClient, client)
}

func TestPostgresDiscover(t *testing.T) {
	client, _, pClient := testPostgresClient(t)
	postgresHelper := base.TestHelper{
		ExecuteQuery: ExecuteQuery,
	}
	base.TestDiscover(t, pClient, client, postgresHelper)
}

func TestPostgresRead(t *testing.T) {
	client, _, pClient := testPostgresClient(t)
	postgresHelper := base.TestHelper{
		ExecuteQuery: ExecuteQuery,
	}
	base.TestRead(t, pClient, client, postgresHelper, func(t *testing.T) (interface{}, protocol.Driver) {
		client, _, mClient := testPostgresClient(t)
		return client, mClient
	})
}
