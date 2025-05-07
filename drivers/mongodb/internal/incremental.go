package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (m *Mongo) incrementalSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	cfg := m.config
	db, collName := stream.Namespace(), stream.Name()
	coll := m.client.Database(db).Collection(collName)

	// ---- hydrated checkpoint from State ------------
	cstream := stream.Self() // *types.ConfiguredStream
	var lastTS primitive.DateTime
	var lastID primitive.ObjectID

	if raw := m.State.GetCursor(cstream, cursorLastTS); raw != nil {
		switch t := raw.(type) {
		case primitive.DateTime:
			lastTS = t
		case string:
			if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
				lastTS = primitive.NewDateTimeFromTime(parsed)
			} else {
				logger.Warnf("could not parse saved last_ts %q: %s", t, err)
			}
		default:
			logger.Warnf("unexpected type for last_ts in state: %T", raw)
		}
	}
	if raw := m.State.GetCursor(cstream, cursorLastID); raw != nil {
		switch v := raw.(type) {
		case primitive.ObjectID:
			lastID = v
		case string:
			if oid, err := primitive.ObjectIDFromHex(v); err == nil {
				lastID = oid
			} else {
				logger.Warnf("could not parse saved last_id %q: %s", v, err)
			}
		default:
			logger.Warnf("unexpected type for last_id in state: %T", raw)
		}
	}

	batch := cfg.BatchSize
	if batch == 0 {
		batch = 5000
	}
	trk := cfg.TrackingField
	if trk == "" {
		ac := stream.GetStream().AvailableCursorFields.Array()
		if len(ac) == 1 {
			trk = ac[0]
		} else {
			trk = "_id"
		}
	}

	logger.Infof("incremental sync started on %s.%s (strategy=%s, batch=%d)", db, collName, cfg.Incremental, batch)

	for {
		var filter bson.M
		findOpts := options.Find().SetBatchSize(batch)

		switch cfg.Incremental {
		case StrategyTimestamp:
			filter = bson.M{trk: bson.M{"$gt": lastTS}}
			findOpts.SetSort(bson.D{{Key: trk, Value: 1}})

		case StrategyObjectID:
			filter = bson.M{"_id": bson.M{"$gt": lastID}}
			findOpts.SetSort(bson.D{{Key: "_id", Value: 1}})

		case StrategySoftDelete:
			filter = bson.M{
				"$or": []bson.M{
					{trk: bson.M{"$gt": lastTS}},
					{"deleted": true, "deletedAt": bson.M{"$gt": lastTS}},
				}}
			findOpts.SetSort(bson.D{{Key: trk, Value: 1}})

		default:
			return fmt.Errorf("unknown incremental strategy %q", cfg.Incremental)
		}

		cur, err := coll.Find(context.TODO(), filter, findOpts)
		if err != nil {
			return err
		}

		thread, err := pool.NewThread(context.TODO(), stream)
		if err != nil {
			cur.Close(context.TODO())
			return err
		}

		var processed int
		for cur.Next(context.TODO()) {
			var doc bson.M
			_ = cur.Decode(&doc)
			handleMongoObject(doc)

			hash := utils.GetKeysHash(doc, constants.MongoPrimaryID)
			if err := thread.Insert(types.CreateRawRecord(hash, doc, "r", time.Unix(0, 0))); err != nil {
				cur.Close(context.TODO())
				return err
			}

			// ---- advance in-memory checkpoint safely ----
			switch cfg.Incremental {
			case StrategyObjectID:
				if hex, ok := doc["_id"].(string); ok {
					if oid, err := primitive.ObjectIDFromHex(hex); err == nil {
						lastID = oid
					}
				}
			default: // timestamp & soft_delete
				// handleMongoObject lower-cases keys → use trk in lower case
				if tsRaw, ok := doc[strings.ToLower(trk)]; ok {
					switch v := tsRaw.(type) {
					case primitive.DateTime:
						lastTS = v
					case time.Time:
						lastTS = primitive.NewDateTimeFromTime(v)
					}
				}
			}
			processed++
		}
		cur.Close(context.TODO())
		thread.Close()

		if processed > 0 {
			m.State.SetCursor(cstream, cursorLastTS, lastTS)
			if cfg.Incremental == StrategyObjectID || cfg.Incremental == StrategySoftDelete {
				m.State.SetCursor(cstream, cursorLastID, lastID)
			}
			m.State.LogState()
		} else {
			return nil
		}
	}
}
