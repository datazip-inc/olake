// File: driver/incremental.go
package driver

import (
	"context"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	bmkField = "cursor_field"
	bmkVal   = "cursor_value"
)

func (m *Mongo) incrementalSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	db, collName := stream.Namespace(), stream.Name()
	coll := m.client.Database(db).Collection(collName)
	cstream := stream.Self()

	cursorField := cstream.CursorField
	if cursorField == "" {
		if ac := stream.GetStream().AvailableCursorFields.Array(); len(ac) == 1 {
			cursorField = ac[0]
		} else {
			cursorField = "_id"
		}
	}
	normCursor := typeutils.Reformat(cursorField)

	stateField := m.State.GetCursor(cstream, bmkField)
	stateVal := m.State.GetCursor(cstream, bmkVal)

	var bookmark any
	if stateVal != nil {
		bookmark = coerceBookmark(stateVal)
	}

	if stateField != cursorField || bookmark == nil {
		logger.Infof("cursor changed (%v→%s) or no bookmark; running back-fill",
			stateField, cursorField)

		if err := m.backfill(stream, pool); err != nil {
			return err
		}
		if err := snapMaxAsBookmark(coll, cursorField, cstream, m); err != nil {
			return err
		}
		return nil
	}

	batch := m.config.BatchSize
	if batch == 0 {
		batch = 5_000
	}

	logger.Infof("incremental sync on %s.%s (cursor=%s, start>%v, batch=%d)",
		db, collName, cursorField, bookmark, batch)

	for {
		cur, err := coll.Find(
			context.TODO(),
			bson.M{cursorField: bson.M{"$gt": bookmark}},
			options.Find().
				SetBatchSize(batch).
				SetSort(bson.D{{Key: cursorField, Value: 1}}),
		)
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
			if err := cur.Decode(&doc); err != nil {
				cur.Close(context.TODO())
				return err
			}

			rawCursor := doc[cursorField]
			handleMongoObject(doc)

			hash := utils.GetKeysHash(doc, constants.MongoPrimaryID)
			if err := thread.Insert(
				types.CreateRawRecord(hash, doc, "r", time.Unix(0, 0)),
			); err != nil {
				cur.Close(context.TODO())
				return err
			}

			var cv any
			if rawCursor != nil {
				cv = rawCursor
			} else if v, ok := doc[normCursor]; ok {
				cv = v
			}
			if cv != nil {
				bookmark = coerceBookmark(cv)
			}
			processed++
		}
		cur.Close(context.TODO())
		thread.Close()

		if processed == 0 {
			return nil
		}

		m.State.SetCursor(cstream, bmkField, cursorField)
		m.State.SetCursor(cstream, bmkVal, bookmark)
		m.State.LogState()
	}
}

func coerceBookmark(v any) any {
	switch t := v.(type) {
	case primitive.DateTime, primitive.ObjectID:
		return t
	case string:
		if oid, err := primitive.ObjectIDFromHex(t); err == nil {
			return oid
		}
		if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return primitive.NewDateTimeFromTime(ts)
		}
	}
	return nil
}

func snapMaxAsBookmark(
	coll *mongo.Collection,
	field string,
	cstream *types.ConfiguredStream,
	m *Mongo,
) error {
	var doc bson.M
	if err := coll.
		FindOne(context.TODO(), bson.D{},
			options.FindOne().SetSort(bson.D{{Key: field, Value: -1}})).
		Decode(&doc); err != nil {
		return err
	}
	if v, ok := doc[field]; ok {
		m.State.SetCursor(cstream, bmkField, field)
		m.State.SetCursor(cstream, bmkVal, v)
		m.State.LogState()
		logger.Infof("bookmark initialised to %v on field %s", v, field)
	}
	return nil
}
