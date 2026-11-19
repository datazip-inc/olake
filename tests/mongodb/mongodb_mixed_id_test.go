package mongodb

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// mixedIDDestination is one destination the mixed _id suite syncs to, with how to empty its table
// and how Spark reads it.
type mixedIDDestination struct {
	name   string
	config string
	reset  func(t *testing.T, db, table string) error
	// source returns what a Spark query reads the table from: a view over the parquet files, or the iceberg table
	source func(ctx context.Context, t *testing.T, db, table string) (string, error)
}

// mixedIDDestinations are Parquet and Iceberg with the legacy (Java) writer.
var mixedIDDestinations = []mixedIDDestination{
	{
		name:   "parquet",
		config: "parquet_s3_destination.json",
		reset:  testutils.DeleteParquetTable,
		source: func(ctx context.Context, t *testing.T, db, table string) (string, error) {
			spark, err := testutils.SparkSession(ctx, t)
			if err != nil {
				return "", err
			}
			view := fmt.Sprintf("`%s_view_%d`", table, time.Now().UnixNano())
			return view, testutils.CreateParquetView(ctx, t, spark, view, db, table)
		},
	},
	{
		name:   "iceberg",
		config: "iceberg_s3_destination.json",
		reset: func(t *testing.T, db, table string) error {
			testutils.DropIcebergTable(t, table, db)
			return nil
		},
		source: func(ctx context.Context, t *testing.T, db, table string) (string, error) {
			spark, err := testutils.SparkSession(ctx, t)
			if err != nil {
				return "", err
			}
			// the shared session caches table snapshots, so refresh to see the rows the sync just committed
			fullTable := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, db, table)
			return fullTable, testutils.RefreshTable(ctx, spark, fullTable)
		},
	},
}

// mixedIDDoc is one document of the mixed _id seed: its _id and the _olake_id the destination must hold for it.
type mixedIDDoc struct {
	id      any
	olakeID string
}

// mixedIDObjectID returns the fixed ObjectID number n of the seed, so chunk bounds can name it.
func mixedIDObjectID(n int) primitive.ObjectID {
	oid, _ := primitive.ObjectIDFromHex(fmt.Sprintf("6500000000000000000000%02d", n))
	return oid
}

// mixedIDCase is one shape of a collection whose _id mixes integers and ObjectIDs.
type mixedIDCase struct {
	name   string
	seedOp string
	ints   int
	oids   int
}

// mixedIDCases are the two shapes seen in production: mostly ObjectIDs with some integers, and
// mostly integers with some ObjectIDs.
var mixedIDCases = []mixedIDCase{
	{name: "mostly ObjectID", seedOp: "add_mixed_id_mostly_objectid", ints: 10, oids: 90},
	{name: "mostly integer", seedOp: "add_mixed_id_mostly_integer", ints: 90, oids: 10},
}

// mixedIDCaseBySeedOp returns the case whose seed operation is op.
func mixedIDCaseBySeedOp(op string) (mixedIDCase, bool) {
	for _, c := range mixedIDCases {
		if c.seedOp == op {
			return c, true
		}
	}
	return mixedIDCase{}, false
}

// docs is the seed of the case: int64 _id values from 1000, then fixed ObjectIDs. Integers sort
// before ObjectIDs in BSON order.
func (c mixedIDCase) docs() []mixedIDDoc {
	docs := make([]mixedIDDoc, 0, c.ints+c.oids)
	for i := range c.ints {
		docs = append(docs, mixedIDDoc{id: int64(1000 + i), olakeID: fmt.Sprint(1000 + i)})
	}
	for i := 1; i <= c.oids; i++ {
		docs = append(docs, mixedIDDoc{id: mixedIDObjectID(i), olakeID: mixedIDObjectID(i).Hex()})
	}
	return docs
}

// insertMixedIDData seeds the collection with docs, each tagged with its position in seq.
func insertMixedIDData(ctx context.Context, t *testing.T, collection *mongo.Collection, docs []mixedIDDoc) {
	t.Helper()
	rows := make([]any, 0, len(docs))
	for seq, doc := range docs {
		rows = append(rows, bson.D{{Key: "_id", Value: doc.id}, {Key: "seq", Value: seq}})
	}
	_, err := collection.InsertMany(ctx, rows)
	require.NoError(t, err, "failed to insert mixed _id documents")
}

// mixedIDChunk is one backfill chunk as OLake stores it in state: numbers as JSON numbers,
// ObjectIDs as hex strings, and a nil max for the last, open chunk.
func mixedIDChunk(minVal, maxVal any) map[string]any {
	return map[string]any{"min": minVal, "max": maxVal}
}

// TestMongodbMixedIDSync syncs collections whose _id mixes integers and ObjectIDs to Parquet and Iceberg,
// with normalization off, and checks that every document lands exactly once with its expected _olake_id.
// The fresh sync cases let OLake chunk the collection itself. The resume cases first write chunks
// whose min and max are of different _id types into the state, the way a backfill of a large mixed
// collection leaves them, and resume from there.
func TestMongodbMixedIDSync(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cfg := mongodbTestConfig(t)
	table := cfg.GetTableName()
	defer func() {
		if testutils.KeepTestData() {
			return
		}
		cfg.ExecuteQuery(context.WithoutCancel(ctx), t, cfg, "drop")
		for _, dest := range mixedIDDestinations {
			if err := dest.reset(t, cfg.DestinationDB, table); err != nil {
				t.Logf("cleanup: failed to reset the %s table: %v", dest.name, err)
			}
		}
	}()

	_, err := cfg.RenderCatalog("")
	require.NoError(t, err, "failed to render the catalog")
	require.NoError(t, cfg.IsolateDestinationDB(), "failed to isolate the destination database")
	require.NoError(t, testutils.UpdateSelectedStreams(cfg, cfg.Namespace, "", "", []string{table}, nil), "failed to select the stream")
	// mixed _id types land in one destination column only when the data stays a JSON document
	require.NoError(t, cfg.Catalog().Edit(func(doc map[string]any) error {
		entry := testutils.SelectedStreamEntry(doc, cfg.Namespace, table)
		if entry == nil {
			return fmt.Errorf("stream %s not selected", table)
		}
		// mixed _id types are supported only with normalization off: with it on, the _id column type comes
		// from discover's sample, and a type the sample missed fails the sync
		entry["normalization"] = false
		return nil
	}), "failed to turn off normalization")

	mostlyObjectID, mostlyInteger := mixedIDCases[0], mixedIDCases[1]
	testCases := []struct {
		name string
		seed mixedIDCase
		// chunks to resume from; nil runs a fresh sync only
		chunks []any
	}{
		{name: "fresh sync, mostly ObjectID", seed: mostlyObjectID},
		{name: "fresh sync, mostly integer", seed: mostlyInteger},
		{
			name: "resume, chunk from integer to ObjectID",
			seed: mostlyInteger,
			chunks: []any{
				mixedIDChunk(1000, 1045),
				mixedIDChunk(1045, mixedIDObjectID(6).Hex()),
				mixedIDChunk(mixedIDObjectID(6).Hex(), nil),
			},
		},
		{
			// the client's state: integer chunks, the last one open past the ObjectIDs
			name: "resume, open chunk from integer",
			seed: mostlyInteger,
			chunks: []any{
				mixedIDChunk(1000, 1045),
				mixedIDChunk(1045, nil),
			},
		},
	}

	// the cases share one collection, so they run one after another
	for _, dest := range mixedIDDestinations {
		t.Run(dest.name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					cfg.ExecuteQuery(ctx, t, cfg, "drop")
					cfg.ExecuteQuery(ctx, t, cfg, "create")
					cfg.ExecuteQuery(ctx, t, cfg, tc.seed.seedOp)
					docs := tc.seed.docs()

					require.NoError(t, dest.reset(t, cfg.DestinationDB, table), "failed to reset the %s table", dest.name)
					require.NoError(t, testutils.RunSync(ctx, t, cfg, dest.config, false), "fresh sync failed")
					verifyMixedIDRows(ctx, t, dest, cfg.DestinationDB, table, docs)
					if tc.chunks == nil {
						return
					}

					// the fresh sync left a state with its resume token and no chunks left to read
					require.NoError(t, setStateChunks(cfg.GetFilePath("state.json"), table, tc.chunks), "failed to write chunks into the state")
					require.NoError(t, dest.reset(t, cfg.DestinationDB, table), "failed to reset the %s table", dest.name)
					require.NoError(t, testutils.RunSync(ctx, t, cfg, dest.config, true), "resumed sync failed")
					verifyMixedIDRows(ctx, t, dest, cfg.DestinationDB, table, docs)
				})
			}
		})
	}
}

// setStateChunks replaces the backfill chunks of a stream in a state file.
func setStateChunks(statePath, table string, chunks []any) error {
	return testutils.EditJSONFile(statePath, func(doc map[string]any) error {
		streams, _ := doc["streams"].([]any)
		for _, raw := range streams {
			stream, ok := raw.(map[string]any)
			if !ok || stream["stream"] != table {
				continue
			}
			state, ok := stream["state"].(map[string]any)
			if !ok {
				return fmt.Errorf("stream %s has no state", table)
			}
			state["chunks"] = chunks
			return nil
		}
		return fmt.Errorf("stream %s not found in state", table)
	})
}

// verifyMixedIDRows checks that the destination table holds every seed document exactly once, read
// by backfill, with the _olake_id the seed expects for its _id type.
func verifyMixedIDRows(ctx context.Context, t *testing.T, dest mixedIDDestination, db, table string, docs []mixedIDDoc) {
	t.Helper()
	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "failed to connect to Spark Connect")
	source, err := dest.source(ctx, t, db, table)
	require.NoError(t, err, "failed to open the %s table", dest.name)

	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT CAST(_olake_id AS STRING) AS olake_id, _op_type, get_json_object(data, '$.seq') AS seq FROM %s", source))
	require.NoError(t, err, "failed to query the %s table", dest.name)
	rows, err := df.Collect(ctx)
	require.NoError(t, err, "failed to collect the %s rows", dest.name)

	seen := make(map[int]bool, len(docs))
	for _, row := range rows {
		seqText, _ := row.Value("seq").(string)
		seq, err := strconv.Atoi(seqText)
		require.NoError(t, err, "row has no valid seq: %v", row.Values())
		require.Truef(t, seq >= 0 && seq < len(docs), "row %v has an unknown seq", row.Values())
		require.Falsef(t, seen[seq], "seq %d synced twice", seq)
		seen[seq] = true

		require.Equal(t, "r", row.Value("_op_type"), "op type of seq %d", seq)
		require.Equal(t, docs[seq].olakeID, row.Value("olake_id"), "_olake_id of seq %d (_id %v)", seq, docs[seq].id)
	}

	var missing []string
	for seq, doc := range docs {
		if !seen[seq] {
			missing = append(missing, fmt.Sprintf("seq %d (_id %T %v)", seq, doc.id, doc.id))
		}
	}
	require.Equal(t, 0, len(missing), "documents missing from the %s table: %v", dest.name, missing)
}
