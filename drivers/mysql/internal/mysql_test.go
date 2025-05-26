package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
)

// Test functions using base utilities
func TestMySQLSetup(t *testing.T) {
	client, _, mClient := testMySQLClient(t)
	base.TestSetup(t, mClient, client)
}

func TestMySQLDiscover(t *testing.T) {
	client, _, mClient := testMySQLClient(t)
	mysqlHelper := base.TestHelper{
		ExecuteQuery: ExecuteQuery,
	}
	base.TestDiscover(t, mClient, client, mysqlHelper)
	// TODO : Add MySQL-specific schema verification if needed
}

func TestMySQLRead(t *testing.T) {
	client, _, mClient := testMySQLClient(t)
	mysqlHelper := base.TestHelper{
		ExecuteQuery: ExecuteQuery,
	}
	base.TestRead(t, mClient, client, mysqlHelper, func(t *testing.T) (interface{}, protocol.Driver) {
		client, _, mClient := testMySQLClient(t)
		base.TestSetup(t, mClient, client)
		return client, mClient
	})
}
