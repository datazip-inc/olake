package driver

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
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

type Range struct {
	Field string
	Min   any
	Max   any
}

type ScanConfig struct {
	Coll       *mongo.Collection
	BatchSize  int32
	Transform  func(bson.M)
	WriterPool *protocol.WriterPool
	MaxThreads int
	RetryCount int
}

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

	snapshotMax, err := getCurrentMax(coll, cursorField)
	if err != nil {
		return fmt.Errorf("snapshot max: %w", err)
	}
	if snapshotMax == nil || compare(snapshotMax, bookmark) <= 0 {
		logger.Infof("nothing new to sync (bookmark=%v, snapshot=%v)", bookmark, snapshotMax)
		return nil
	}

	chunks, err := splitIncrementalChunks(ctx, coll, cursorField, bookmark, snapshotMax, m.config.MaxThreads*4)
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		chunks = []Range{{Field: cursorField, Min: bookmark, Max: snapshotMax}}
	}

	logger.Infof("incremental sync on %s.%s (%d chunks, cursor=%s, start≥%v)",
		db, collName, len(chunks), cursorField, bookmark)

	processChunk := func(rng Range) error {
		w, err := pool.NewThread(ctx, stream, protocol.WithBackfill(true))
		if err != nil {
			return err
		}
		defer w.Close()

		return scanIntoWriter(ctx, coll, rng, int32(m.effectiveBatch()), w)
	}

	if err := utils.Concurrent(ctx, chunks, m.config.MaxThreads, func(ctx context.Context, rng Range, _ int) error {
		return processChunk(rng)
	}); err != nil {
		cancel()
		return err
	}

	m.State.SetCursor(cstream, bmkField, cursorField)
	m.State.SetCursor(cstream, bmkVal, snapshotMax)
	m.State.LogState()
	return nil
}
func scanIntoWriter(
	ctx context.Context,
	coll *mongo.Collection,
	rng Range,
	batchSize int32,
	writer *protocol.ThreadEvent,
) error {

	filter := bson.M{}
	if rng.Min != nil {
		filter[rng.Field] = bson.M{"$gt": rng.Min}
	}
	if rng.Max != nil {
		m, ok := filter[rng.Field].(bson.M)
		if !ok {
			m = bson.M{}
		}
		m["$lte"] = rng.Max
		filter[rng.Field] = m
	}
	opts := options.Find().
		SetSort(bson.D{{Key: rng.Field, Value: 1}}).
		SetBatchSize(batchSize).
		SetHint(bson.D{{Key: rng.Field, Value: 1}})

	return base.RetryOnBackoff(3, time.Minute, func() error {
		cur, err := coll.Find(ctx, filter, opts)
		if err != nil {
			return err
		}
		defer cur.Close(ctx)

		for cur.Next(ctx) {
			var doc bson.M
			if err := cur.Decode(&doc); err != nil {
				return err
			}
			handleMongoObject(doc)
			hash := utils.GetKeysHash(doc, constants.MongoPrimaryID)
			if err := writer.Insert(
				types.CreateRawRecord(hash, doc, "r", time.Unix(0, 0)),
			); err != nil {
				return err
			}
		}
		return cur.Err()
	})
}

func (m *Mongo) effectiveBatch() int {
	if m.config.BatchSize > 0 {
		return int(m.config.BatchSize)
	}
	return defaultBatch
}

func getCurrentMax(coll *mongo.Collection, field string) (any, error) {
	var doc bson.M
	if err := coll.FindOne(
		context.TODO(),
		bson.D{},
		options.FindOne().SetSort(bson.D{{Key: field, Value: -1}}),
	).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return doc[field], nil
}

func compare(a, b any) int {
	switch x := a.(type) {
	case primitive.DateTime:
		y := b.(primitive.DateTime)
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
		return 0
	case primitive.ObjectID:
		y := b.(primitive.ObjectID)
		return bytes.Compare(x[:], y[:])
	default:
		return 0
	}
}

func splitIncrementalChunks(
	ctx context.Context,
	coll *mongo.Collection,
	field string,
	min any,
	max any,
	targetBuckets int,
) ([]Range, error) {

	match := bson.D{{Key: field, Value: bson.D{
		{Key: "$gt", Value: min},
		{Key: "$lte", Value: max},
	}}}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$bucketAuto", Value: bson.D{
			{Key: "groupBy", Value: "$" + field},
			{Key: "buckets", Value: targetBuckets},
		}}},
	}

	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var buckets []struct {
		ID struct {
			Min any `bson:"min"`
			Max any `bson:"max"`
		} `bson:"_id"`
	}
	if err := cur.All(ctx, &buckets); err != nil {
		return nil, err
	}

	if len(buckets) == 0 {
		return []Range{{Field: field, Min: min, Max: max}}, nil
	}

	parts := make([]Range, 0, len(buckets)+1)
	prev := min
	for _, b := range buckets {
		parts = append(parts, Range{
			Field: field,
			Min:   prev,
			Max:   b.ID.Max,
		})
		prev = b.ID.Max
	}
	if compare(prev, max) < 0 {
		parts = append(parts, Range{
			Field: field,
			Min:   prev,
			Max:   max,
		})
	}

	return parts, nil
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
			if e := writer.Insert(types.CreateRawRecord(hash, doc, "r", time.Unix(0, 0))); e != nil {
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

func (sc *ScanConfig) ScanRange(
	ctx context.Context,
	rng Range,
	stream protocol.Stream,
) (lastSeen any, err error) {

	filter := bson.M{}
	if rng.Min != nil {
		filter[rng.Field] = bson.M{"$gte": rng.Min}
	}
	if rng.Max != nil {
		if m, ok := filter[rng.Field].(bson.M); ok {
			m["$lt"] = rng.Max
		} else {
			filter[rng.Field] = bson.M{"$lt": rng.Max}
		}
	}
	opts := options.Find().
		SetSort(bson.D{{Key: rng.Field, Value: 1}}).
		SetBatchSize(sc.BatchSize).
		SetHint(bson.D{{Key: rng.Field, Value: 1}})

	if sc.MaxThreads <= 0 {
		sc.MaxThreads = 1
	}
	docsCh := make(chan bson.M, 32*sc.MaxThreads)
	g, ctx := errgroup.WithContext(ctx)
	spawnWriters(g, ctx, sc.MaxThreads, stream, sc.WriterPool, docsCh)

	g.Go(func() error {
		return base.RetryOnBackoff(sc.RetryCount, time.Minute, func() error {
			cur, e := sc.Coll.Find(ctx, filter, opts)
			if e != nil {
				return e
			}
			defer cur.Close(ctx)

			for cur.Next(ctx) {
				var doc bson.M
				if e := cur.Decode(&doc); e != nil {
					return e
				}
				sc.Transform(doc)
				lastSeen = doc[rng.Field]

				select {
				case docsCh <- doc:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if e := cur.Err(); e != nil {
				return e
			}
			close(docsCh)
			return nil
		})
	})

	err = g.Wait()
	return
}
