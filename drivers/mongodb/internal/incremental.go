package driver

import (
	"context"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"golang.org/x/sync/errgroup"
)

const (
	bmkField        = "cursor_field"
	bmkVal          = "cursor_value"
	defaultBatch    = 5_000
	defaultChkEvery = 50_000
)

func (m *Mongo) incrementalSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, collName := stream.Namespace(), stream.Name()
	coll := m.client.
		Database(db, options.Database().SetReadConcern(readconcern.Majority())).
		Collection(collName)

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
		logger.Infof("cursor changed (%v→%s) or empty bookmark – switching to back-fill",
			stateField, cursorField)
		if err := m.backfill(stream, pool); err != nil {
			return err
		}
		return snapMaxAsBookmark(coll, cursorField, cstream, m)
	}

	batch := m.config.BatchSize
	if batch == 0 {
		batch = defaultBatch
	}
	logger.Infof("incremental sync on %s.%s (cursor=%s, start≥%v, batch=%d)",
		db, collName, cursorField, bookmark, batch)

	threads := m.config.MaxThreads
	if threads == 0 {
		threads = 1
	}

	docsCh := make(chan bson.M, 32*threads)
	g, ctx := errgroup.WithContext(ctx)
	spawnWriters(g, ctx, threads, stream, pool, docsCh)
	start := bookmark
	lastSeen := start

	g.Go(func() error {
		opts := options.Find().
			SetSort(bson.D{{Key: cursorField, Value: 1}}).
			SetBatchSize(int32(batch)).
			SetHint(bson.D{{Key: cursorField, Value: 1}})

		var processed int64

		return base.RetryOnBackoff(m.config.RetryCount, time.Minute, func() error {
			filter := bson.M{cursorField: bson.M{"$gte": start}}

			cur, err := coll.Find(ctx, filter, opts)
			if err != nil {
				return err
			}
			defer cur.Close(ctx)

			lastSeen = start
			for cur.Next(ctx) {
				var doc bson.M
				if err := cur.Decode(&doc); err != nil {
					return err
				}
				rawCursor := doc[cursorField]
				handleMongoObject(doc)

				select {
				case docsCh <- doc:
				case <-ctx.Done():
					return ctx.Err()
				}

				if rawCursor != nil {
					lastSeen = coerceBookmark(rawCursor)
				} else if v, ok := doc[normCursor]; ok {
					lastSeen = coerceBookmark(v)
				}

				processed++
				if processed%defaultChkEvery == 0 {
					m.State.SetCursor(cstream, bmkVal, lastSeen)
					m.State.LogState()
				}
			}
			if err := cur.Err(); err != nil {
				return err
			}

			close(docsCh)

			logger.Infof("incremental bookmark advanced to %v on field %s", lastSeen, cursorField)
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		cancel()
		return err
	}
	m.State.SetCursor(cstream, bmkField, cursorField)
	m.State.SetCursor(cstream, bmkVal, lastSeen)
	m.State.LogState()
	return nil
}

func spawnWriters(
	g *errgroup.Group,
	ctx context.Context,
	n int,
	stream protocol.Stream,
	pool *protocol.WriterPool,
	in <-chan bson.M,
) {
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		g.Go(func() error { return writeWorker(ctx, stream, pool, in) })
	}
}

func writeWorker(
	ctx context.Context,
	stream protocol.Stream,
	pool *protocol.WriterPool,
	in <-chan bson.M,
) error {
	writer, err := pool.NewThread(ctx, stream, protocol.WithBackfill(false))
	if err != nil {
		return err
	}
	defer writer.Close()

	for {
		select {
		case doc, ok := <-in:
			if !ok {
				return nil
			}
			hash := utils.GetKeysHash(doc, constants.MongoPrimaryID)
			if e := writer.Insert(
				types.CreateRawRecord(hash, doc, "r", time.Unix(0, 0)),
			); e != nil {
				return e
			}
		case <-ctx.Done():
			return ctx.Err()
		}
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
	if err := coll.FindOne(
		context.TODO(),
		bson.D{},
		options.FindOne().SetSort(bson.D{{Key: field, Value: -1}}),
	).Decode(&doc); err != nil {
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
