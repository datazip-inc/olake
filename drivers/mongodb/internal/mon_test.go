package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
)

// Test functions using base utilities
func TestMongoSetup(t *testing.T) {
	client, _, mClient := testMongoClient(t)
	base.TestSetup(t, mClient, client)
}

func TestMongoDiscover(t *testing.T) {
	client, _, mClient := testMongoClient(t)
	helper := base.TestHelper{
		CreateTable:  createTestCollection,
		DropTable:    dropTestCollection,
		CleanTable:   cleanTestCollection,
		AddMongoData: addTestDocuments,
	}
	base.TestDiscover(t, mClient, client, helper)
}

func TestMongoRead(t *testing.T) {
	client, _, mClient := testMongoClient(t)
	helper := base.TestHelper{
		CreateTable:  createTestCollection,
		DropTable:    dropTestCollection,
		CleanTable:   cleanTestCollection,
		AddMongoData: addTestDocuments,
		InsertOp:     insertDocOp,
		UpdateOp:     updateDocOp,
		DeleteOp:     deleteDocOp,
	}
	base.TestRead(t, mClient, client, helper, func(t *testing.T) (interface{}, protocol.Driver) {
		client, _, mClient := testMongoClient(t)
		base.TestSetup(t, mClient, client)
		return client, mClient
	})
}
