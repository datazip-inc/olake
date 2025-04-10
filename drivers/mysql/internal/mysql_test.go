package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
)

// SetupClient adapter for base package
func setupClient(t *testing.T) (interface{}, protocol.Driver) {
	client, _, mClient := testMySQLClient(t)
	return client, mClient
}

// Test functions using base utilities
func TestMySQLSetup(t *testing.T) {
	client, _, mClient := testMySQLClient(t)
	base.TestSetup(t, mClient, client)
}

func TestMySQLDiscover(t *testing.T) {
	client, _, mClient := testMySQLClient(t)
	helper := base.TestHelper{
		CreateTable: createTestTable,
		DropTable:   dropTestTable,
		CleanTable:  cleanTestTable,
		AddData:     addTestTableData,
	}
	base.TestDiscover(t, mClient, client, helper)
	// TODO : Add MySQL-specific schema verification if needed
}

func TestMySQLRead(t *testing.T) {
	client, _, mClient := testMySQLClient(t)
	helper := base.TestHelper{
		CreateTable: createTestTable,
		DropTable:   dropTestTable,
		CleanTable:  cleanTestTable,
		AddData:     addTestTableData,
		InsertOp:    insertOp,
		UpdateOp:    updateOp,
		DeleteOp:    deleteOp,
	}
	base.TestRead(t, mClient, client, helper, setupClient)
}
