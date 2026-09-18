package driver

import (
	"context"
	"errors"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// The MongoDB config has no CDC selection yet, so checks must not block full-refresh users.
// Flip once the config carries CDC intent.
const cdcIntentInConfig = false

const (
	prerequisiteTimeout = 30 * time.Second

	errCodeUnauthorized          = 13
	errCodeChangeStreamsDisabled = 40573 // not a replica set / sharded cluster
)

func (m *Mongo) prerequisiteChecks() []abstract.Prerequisite {
	return []abstract.Prerequisite{
		{
			Name: "change_streams", Required: cdcIntentInConfig,
			Recommended: "replica set or sharded cluster, read role on the database",
			Description: "OLake cannot open a change stream, so CDC cannot run.",
			Check:       m.checkChangeStreams,
		},
		{
			Name: "oplog_retention", Recommended: ">= " + utils.HumanDuration(constants.RecommendedCDCLogRetention),
			Description: "If a sync is paused longer than the oplog window, its resume point is lost and a full resync is required.",
			Check:       m.checkOplogRetention,
		},
	}
}

// checkChangeStreams opens and closes a change stream. It proves the topology and the
// find/changeStream privileges in one call.
func (m *Mongo) checkChangeStreams(ctx context.Context) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, prerequisiteTimeout)
	defer cancel()

	stream, err := m.client.Database(m.config.Database).Watch(ctx, mongo.Pipeline{})
	if err != nil {
		var cmdErr mongo.CommandError
		if errors.As(err, &cmdErr) {
			switch cmdErr.Code {
			case errCodeChangeStreamsDisabled:
				return "standalone server", false, nil
			case errCodeUnauthorized:
				return "not authorized", false, nil
			}
		}
		return "", false, err
	}
	_ = stream.Close(ctx)
	return "available", true, nil
}

// checkOplogRetention measures the oplog window as newest minus oldest entry, the same
// arithmetic as the shell's rs.printReplicationInfo(). Needs read on local; on mongos the
// read errors and the check reports unavailable.
func (m *Mongo) checkOplogRetention(ctx context.Context) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, prerequisiteTimeout)
	defer cancel()

	oplog := m.client.Database("local").Collection("oplog.rs")
	edge := func(direction int) (primitive.Timestamp, error) {
		var entry struct {
			TS primitive.Timestamp `bson:"ts"`
		}
		opts := options.FindOne().SetSort(bson.D{{Key: "$natural", Value: direction}})
		err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&entry)
		return entry.TS, err
	}

	first, err := edge(1)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "no oplog (standalone)", false, nil
	}
	if err != nil {
		return "", false, err
	}
	last, err := edge(-1)
	if err != nil {
		return "", false, err
	}

	window := time.Duration(int64(last.T)-int64(first.T)) * time.Second
	return utils.HumanDuration(window), window >= constants.RecommendedCDCLogRetention, nil
}

// Prerequisites returns the CDC setup checks evaluated in Setup.
func (m *Mongo) Prerequisites() types.Prerequisites {
	return m.prerequisites
}
