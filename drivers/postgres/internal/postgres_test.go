package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
)

// SetupClient adapter for base package
func setupClient(t *testing.T) (interface{}, protocol.Driver) {
	client, _, mClient := testPostgresClient(t)
	return client, mClient
}

// Test functions using base utilities
func TestMySQLSetup(t *testing.T) {
	client, _, pClient := testPostgresClient(t)
	base.TestSetup(t, pClient, client)
}

func TestMySQLDiscover(t *testing.T) {
	client, _, pClient := testPostgresClient(t)
	helper := base.TestHelper{
		CreateTable: createTestTable,
		DropTable:   dropTestTable,
		CleanTable:  cleanTestTable,
		AddData:     addTestTableData,
	}
	base.TestDiscover(t, pClient, client, helper)
	// TODO : Add Postgres-specific schema verification if needed
}

func TestMySQLRead(t *testing.T) {
	client, _, pClient := testPostgresClient(t)
	helper := base.TestHelper{
		CreateTable: createTestTable,
		DropTable:   dropTestTable,
		CleanTable:  cleanTestTable,
		AddData:     addTestTableData,
		InsertOp:    insertOp,
		UpdateOp:    updateOp,
		DeleteOp:    deleteOp,
	}
	base.TestRead(t, pClient, client, helper, setupClient)
}
