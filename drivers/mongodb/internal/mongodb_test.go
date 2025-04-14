package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"go.mongodb.org/mongo-driver/mongo"
)

// SetupMongoClient adapter for base package
func setupMongoClient(t *testing.T) (*mongo.Client, protocol.Driver) {
	client, _, mClient := testMongoClient(t)
	return client, mClient
}

// Test functions using base utilities
func TestMongoSetup(t *testing.T) {
	client, _, mClient := testMongoClient(t)
	base.TestMongoSetup(t, mClient, client)
}

func TestMongoDiscover(t *testing.T) {
	client, config, mClient := testMongoClient(t)
	helper := base.MongoTestHelper{
		CreateCollection: createTestCollection,
		DropCollection:   dropTestCollection,
		CleanCollection:  cleanTestCollection,
		AddDocuments:     addTestDocuments,
	}
	base.TestMongoDiscover(t, mClient, client, helper, config.Database)
}

func TestMongoRead(t *testing.T) {
	client, config, mClient := testMongoClient(t)
	helper := base.MongoTestHelper{
		CreateCollection: createTestCollection,
		DropCollection:   dropTestCollection,
		CleanCollection:  cleanTestCollection,
		AddDocuments:     addTestDocuments,
		InsertDocOp:      insertDocOp,
		UpdateDocOp:      updateDocOp,
		DeleteDocOp:      deleteDocOp,
	}
	base.TestMongoRead(t, mClient, client, helper, setupMongoClient, config.Database)
}
