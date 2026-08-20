package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/stretchr/testify/require"
)

// The S3 integration test reuses the MinIO instance from the Iceberg destination stack
// (destination/iceberg/local-test/docker-compose.yml) as the source object store. Every
// format variant shares one bucket, isolated by the path prefix in its source.json.
const (
	rowsPerFile = 3

	// S3DestinationDB is the Iceberg database every variant syncs into: discover derives it
	// as <driver type>_<bucket>:<namespace> and reformats it to underscores. Tables stay
	// distinct per variant through the stream name.
	S3DestinationDB = "s3_olake_s3_test_s3"

	// S3CursorField is the cursor discover exposes on every S3 stream: the file's
	// LastModified timestamp. An incremental sync re-reads only the files stamped after the
	// cursor stored by the previous sync.
	S3CursorField = "_last_modified_time"

	// S3PartitionRegex partitions the destination table by str_col. Any source column would
	// do — the test asserts the partition column reached the destination, not the layout.
	S3PartitionRegex = "/{str_col, identity}"

	// evolvedColumn is the column the "evolve-schema" operation introduces: it is absent
	// from the catalog discover built, so the update sync only passes if the pipeline
	// carries it through (sync_new_columns) and the destination adds the column. The
	// parsers hand an unseen column through as a string, so string is the type it lands as.
	evolvedColumn      = "new_col"
	evolvedColumnValue = "evolved"

	// excludedColumn is deselected from the stream before every sync: discover must see it,
	// the destination must not carry it.
	excludedColumn      = "excluded_col"
	excludedColumnValue = "excluded"
)

// farFutureTS rides in the Parquet variant's ts_far_col, same instant in every file: a
// timestamp whose micros overflow int64 nanoseconds, pinning the parser against the scaling
// bug that wrapped 9999-12-31 back to year 1816 (see parquet_test.go "far future").
var farFutureTS = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// S3FilterConfig is satisfied by every seeded row, matching how the other drivers exercise
// filters: it proves the filter is parsed and applied without dropping the rows the sync is
// verified against. S3 filters in memory after read (constants.FullRefreshPostReadFilterDrivers)
// since there is no query to push them down into.
const S3FilterConfig = `{
	"logical_operator": "And",
	"conditions": [
		{
			"column": "str_col",
			"operator": "!=",
			"value": ""
		},
		{
			"column": "float_col",
			"operator": "<",
			"value": 100.00
		}
	]
}`

// rowValues are the business-column values shared by every row of one seeded file. Only
// "id" varies per row, so each record hashes to a distinct _olake_id (S3 streams have no
// primary key, so the whole record is hashed).
//
// The CSV and JSON variants carry what a text format can express -- strings, booleans,
// numbers (Float and Int64, both inferred as double) and the four timestamp precisions
// (TS/TSMilli/TSMicro/TSNano), plus JSON and List for the JSON variant, whose format can
// nest objects and arrays. The remaining fields exist for the Parquet variant alone.
type rowValues struct {
	Str   string
	Bool  bool
	Float float64
	TS    time.Time

	TSMilli time.Time
	TSMicro time.Time
	TSNano  time.Time

	Int8  int8
	Int16 int16
	Int32 int32
	Int64 int64

	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64

	Float32 float32

	Unicode string
	Bytes   []byte
	JSON    string
	Enum    string
	UUID    [16]byte
	Dec32   int32
	Dec64   int64

	TimeOfDay time.Duration
	List      []string
}

var (
	// seedValues are carried by the files "add" and "insert" upload, so a row is verified
	// against the same expectation whether it arrived through backfill or incrementally.
	//
	// The integer columns carry the extremes of their range rather than a round number:
	// every one of them is stored in a wider signed physical column, so a value that only
	// exercises the low bits would not catch a width or sign error.
	//
	// TS stops at whole seconds and TSMilli at whole milliseconds, which every destination
	// carries identically. TSMicro and TSNano hold finer digits, where the destinations
	// genuinely differ -- the legacy Iceberg writer ships timestamptz as epoch millis
	// (toProtoFieldValue, legacy-writer/writer.go) while the Arrow writer and the Parquet
	// destination keep micros -- so their columns are asserted against the writer that
	// actually ran (see textWriterExpectedData and parquetWriterExpectedData).
	seedValues = rowValues{
		Str:   "test_string",
		Bool:  true,
		Float: 99.99,
		TS:    time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC),

		TSMilli: time.Date(2024, 6, 15, 10, 30, 0, 123_000_000, time.UTC),
		TSMicro: time.Date(2024, 6, 15, 10, 30, 0, 123_456_000, time.UTC),
		TSNano:  time.Date(2024, 6, 15, 10, 30, 0, 123_456_789, time.UTC),

		Int8:  math.MinInt8,
		Int16: math.MinInt16,
		Int32: math.MinInt32,
		Int64: math.MinInt64,

		Uint8:  math.MaxUint8,
		Uint16: math.MaxUint16,
		Uint32: math.MaxUint32,
		// Not MaxUint64: no signed type is wide enough to carry it, so the parser hands on
		// a negative, the same way the MySQL driver carries "unsigned bigint".
		Uint64: math.MaxInt64,

		Float32: 1.5,

		Unicode: "héllo→世界🎉",
		Bytes:   []byte{0xff, 0xfe, 0x00},
		JSON:    `{"key":"value"}`,
		Enum:    "ENUM_A",
		UUID:    [16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00},
		Dec32:   12345,
		Dec64:   1234567,

		TimeOfDay: 10*time.Hour + 30*time.Minute,
		List:      []string{"a", "b"},
	}

	// updatedValues are carried by the file "update" uploads, and differ from seedValues in
	// every column so the incremental sync is verified to have picked up that file alone.
	// Where seedValues sit at one end of a type's range, these sit at the other.
	updatedValues = rowValues{
		Str:   "updated_string",
		Bool:  false,
		Float: 11.11,
		TS:    time.Date(2025, 1, 20, 8, 45, 0, 0, time.UTC),

		TSMilli: time.Date(2025, 1, 20, 8, 45, 0, 987_000_000, time.UTC),
		TSMicro: time.Date(2025, 1, 20, 8, 45, 0, 987_654_000, time.UTC),
		TSNano:  time.Date(2025, 1, 20, 8, 45, 0, 987_654_321, time.UTC),

		Int8:  math.MaxInt8,
		Int16: math.MaxInt16,
		Int32: math.MaxInt32,
		Int64: math.MaxInt64,

		Uint8:  0,
		Uint16: 0,
		Uint32: 0,
		Uint64: 0,

		Float32: -2.5,

		Unicode: "updated→更新🎊",
		Bytes:   []byte{0x00, 0x01, 0x80},
		JSON:    `{"key":"updated"}`,
		Enum:    "ENUM_B",
		UUID:    [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		Dec32:   -6789,
		Dec64:   -7654321,

		TimeOfDay: 8*time.Hour + 45*time.Minute,
		List:      []string{"x", "y"},
	}

	// S3CSVToDestinationSchema and S3JSONToDestinationSchema are the expected destination
	// schemas for the two text variants, whose parsers infer every number as double. They
	// share the text columns and differ where the formats do: only CSV can carry an
	// all-empty column, only JSON can carry missing fields and nested values.
	S3CSVToDestinationSchema = s3TextDestinationSchema(map[string]string{
		"null_col": "string",
	})
	S3JSONToDestinationSchema = s3TextDestinationSchema(map[string]string{
		"optional_col": "string",
		"object_col":   "json",
		"array_col":    "array",
	})

	// S3CSVUpdatedDestinationSchema and S3JSONUpdatedDestinationSchema are the destination
	// schemas after the "evolve-schema" operation shipped a file carrying evolvedColumn.
	S3CSVUpdatedDestinationSchema  = evolvedSchema(S3CSVToDestinationSchema)
	S3JSONUpdatedDestinationSchema = evolvedSchema(S3JSONToDestinationSchema)

	// S3ParquetToDestinationSchema is the expected destination schema for Parquet sources,
	// which carry their own schema rather than having one inferred from text. Keys are the
	// driver-side type names testutils.GlobalTypeMapping resolves to an Iceberg type.
	S3ParquetToDestinationSchema = map[string]string{
		"id":       "bigint",
		"bool_col": "boolean",

		// Signed 8/16/32 bit integers all narrow to int; only 64 bit stays bigint.
		"int8_col":  "int",
		"int16_col": "int",
		"int32_col": "int",
		"int64_col": "bigint",

		// Unsigned 32 bit widens to bigint so its top half does not read negative;
		// unsigned 64 has nothing wider to widen into and stays bigint.
		"uint8_col":  "int",
		"uint16_col": "int",
		"uint32_col": "bigint",
		"uint64_col": "bigint",

		"float32_col": "float",
		"float_col":   "double",

		"str_col":     "string",
		"unicode_col": "string",
		"empty_col":   "string",
		"null_col":    "string",
		"bytes_col":   "string",
		"json_col":    "string",
		"enum_col":    "string",
		"uuid_col":    "string",

		// Decimals are carried as double: the parser resolves the scale itself.
		"dec32_col":     "double",
		"dec64_col":     "double",
		"dec_bytes_col": "double",

		"date_col": "timestamp",
		// Times arrive as a count of seconds, not a point in time.
		"time_ms_col": "bigint",
		"time_us_col": "bigint",
		"time_ns_col": "bigint",
		"ts_ms_col":   "timestamp",
		"ts_col":      "timestamp",
		"ts_ns_col":   "timestamp",
		"ts_far_col":  "timestamp",
		"int96_col":   "timestamp",

		// The flattener lands every nested value as a string column.
		"map_col":    "json",
		"struct_col": "json",
		"list_col":   "array",

		"_last_modified_time": "string",
	}

	// S3ParquetUpdatedDestinationSchema is the destination schema after the
	// "evolve-schema" operation shipped a file carrying evolvedColumn.
	S3ParquetUpdatedDestinationSchema = evolvedSchema(S3ParquetToDestinationSchema)
)

// evolvedSchema is base plus the string column the "evolve-schema" operation introduces.
func evolvedSchema(base map[string]string) map[string]string {
	schema := maps.Clone(base)
	schema[evolvedColumn] = "string"
	return schema
}

// s3TextDestinationSchema is the destination schema shared by the CSV and JSON variants,
// extended with the given format-specific columns.
func s3TextDestinationSchema(formatSpecific map[string]string) map[string]string {
	schema := map[string]string{
		"id":        "double",
		"str_col":   "string",
		"bool_col":  "boolean",
		"float_col": "double",
		"int_col":   "double",
		"mixed_col": "string",
		"date_col":  "timestamp",

		"ts_col":       "timestamp",
		"ts_milli_col": "timestamp",
		"ts_micro_col": "timestamp",
		"ts_nano_col":  "timestamp",

		"_last_modified_time": "string",
	}
	maps.Copy(schema, formatSpecific)
	return schema
}

// expectedTextData is the expectation shared by the CSV and JSON variants. Absent on
// purpose: mixed_col cycles its value per row (that is what makes it mixed) and
// optional_col is missing from some rows, so neither has one value every row must carry;
// ts_micro_col and ts_nano_col live in textWriterExpectedData because their synced value
// depends on which destination writer ran (see seedValues).
func expectedTextData(v rowValues) map[string]interface{} {
	return map[string]interface{}{
		"str_col":   v.Str,
		"bool_col":  v.Bool,
		"float_col": v.Float,
		// Both text parsers infer integer-form numbers as double, so the extreme int64
		// arrives through the same float64 rounding the parser applied.
		"int_col":  float64(v.Int64),
		"date_col": arrow.Timestamp(v.TS.UTC().Truncate(24 * time.Hour).UnixMicro()),
		// buildCSVFile and buildJSONLFile render ts_col with time.RFC3339, which
		// carries no fractional seconds. The seed is whole seconds anyway; Truncate keeps
		// this expectation honest should the seed ever grow a sub-second part.
		"ts_col":       arrow.Timestamp(v.TS.Truncate(time.Second).UnixMicro()),
		"ts_milli_col": arrow.Timestamp(v.TSMilli.UnixMicro()),
	}
}

func expectedCSVData(v rowValues) map[string]interface{} {
	data := expectedTextData(v)
	data["null_col"] = nil
	return data
}

func expectedJSONData(v rowValues) map[string]interface{} {
	data := expectedTextData(v)
	// The destination flattener JSON-encodes objects and arrays alike before any writer
	// sees them, so both nested columns pin exact compact JSON. The seed object is compact
	// single-key JSON already, so parsing and re-encoding it reproduces the input.
	data["object_col"] = v.JSON
	data["array_col"] = mustJSON(v.List)
	return data
}

// textWriterExpectedData is the writer-dependent slice of the CSV and JSON expectations:
// below the millisecond the destinations part ways, the legacy Iceberg writer truncating
// every timestamptz to epoch millis where the Arrow writer and the Parquet destination
// keep micros. applyWriterExpectations merges it over the variant's expected data before
// every operation, so each sync is asserted against the precision its writer actually
// produces rather than the shared floor.
func textWriterExpectedData(v rowValues, writer s3DestinationWriter) map[string]interface{} {
	// Truncate mirrors the writers, which floor rather than round: UnixMilli in the
	// legacy writer, UnixMicro in the Arrow writer and parquet-go.
	keep := time.Microsecond
	if writer == writerLegacy {
		keep = time.Millisecond
	}
	return map[string]interface{}{
		"ts_micro_col": arrow.Timestamp(v.TSMicro.Truncate(keep).UnixMicro()),
		"ts_nano_col":  arrow.Timestamp(v.TSNano.Truncate(keep).UnixMicro()),
	}
}

// expectedParquetData is what every synced row of the Parquet variant must carry. The Go
// types are the ones Spark hands back for each Iceberg type: int32 for int, int64 for
// bigint, float32 for float, and arrow.Timestamp (microseconds) for timestamp.
func expectedParquetData(v rowValues) map[string]interface{} {
	day := v.TS.UTC().Truncate(24 * time.Hour)
	return map[string]interface{}{
		"bool_col": v.Bool,

		"int8_col":  int32(v.Int8),
		"int16_col": int32(v.Int16),
		"int32_col": v.Int32,
		"int64_col": v.Int64,

		"uint8_col":  int32(v.Uint8),
		"uint16_col": int32(v.Uint16),
		"uint32_col": int64(v.Uint32),
		//nolint:gosec // G115: mirrors the parser, which cannot widen unsigned 64 any further
		"uint64_col": int64(v.Uint64),

		"float32_col": v.Float32,
		"float_col":   v.Float,

		"str_col":     v.Str,
		"unicode_col": v.Unicode,
		"empty_col":   "",
		"null_col":    nil,
		// Not valid UTF-8, so the parser base64 encodes it rather than corrupting it.
		"bytes_col": base64.StdEncoding.EncodeToString(v.Bytes),
		"json_col":  v.JSON,
		"enum_col":  v.Enum,
		"uuid_col":  uuid.UUID(v.UUID).String(),

		"dec32_col":     float64(v.Dec32) / 100,    // scale 2
		"dec64_col":     float64(v.Dec64) / 10_000, // scale 4
		"dec_bytes_col": float64(v.Dec32) / 100,    // scale 2, byte-array physical form

		"date_col": arrow.Timestamp(day.UnixMicro()),
		// A time is a duration into the day, truncated to whole seconds by the parser.
		"time_ms_col": int64(v.TimeOfDay.Seconds()),
		"time_us_col": int64(v.TimeOfDay.Seconds()),
		"time_ns_col": int64(v.TimeOfDay.Seconds()),
		// The millisecond seed survives identically through every writer (micros and
		// millis renderings agree at this precision), so it stays a shared expectation;
		// the micro, nano and Int96 columns are writer-dependent and live in
		// parquetWriterExpectedData.
		"ts_ms_col":  arrow.Timestamp(v.TSMilli.UnixMicro()),
		"ts_far_col": arrow.Timestamp(farFutureTS.UnixMicro()),

		// The destination flattener (utils/typeutils/flatten.go) JSON-encodes every
		// non-scalar value before it reaches the writer, so the map, struct and array all
		// arrive as compact JSON strings. This matches how the MongoDB driver carries
		// nested values.
		"map_col":    mustJSON(map[string]interface{}{"k": v.Str}),
		"struct_col": mustJSON(map[string]interface{}{"a": v.Str, "b": v.Int64}),
		"list_col":   mustJSON(v.List),
	}
}

// parquetWriterExpectedData is the writer-dependent slice of the Parquet variant's
// expectations, the same split textWriterExpectedData makes for the text variants: the
// sub-millisecond digits these columns carry survive only where the writer keeps micros.
func parquetWriterExpectedData(v rowValues, writer s3DestinationWriter) map[string]interface{} {
	keep := time.Microsecond
	if writer == writerLegacy {
		keep = time.Millisecond
	}
	return map[string]interface{}{
		"ts_col":    arrow.Timestamp(v.TSMicro.Truncate(keep).UnixMicro()),
		"ts_ns_col": arrow.Timestamp(v.TSNano.Truncate(keep).UnixMicro()),
		"int96_col": arrow.Timestamp(v.TSNano.Truncate(keep).UnixMicro()),
	}
}

// mustJSON renders v the way the destination flattener does: compact JSON. The flattener
// uses goccy/go-json, but for a slice of plain strings the encoders are byte-identical.
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// buildFileFn renders rowsPerFile rows carrying vals, with ids startID..startID+rowsPerFile-1.
type buildFileFn func(t *testing.T, startID int64, vals rowValues) []byte

// S3TestVariant describes one source file format exercised by TestS3Integration. Each
// variant owns a testdata/<DataFormat>/ directory holding its committed source.json,
// destination configs and expected discover output, plus a folder inside the shared bucket.
type S3TestVariant struct {
	Name       string
	DataFormat string // testdata subdirectory, and part of the stream name
	PlainExt   string // extension of an uncompressed file of this format
	// Gzipped seeds the stream's second file gzipped, alongside a plain first one. The
	// driver detects compression from each file's own extension, so one stream can mix both.
	Gzipped   bool
	BuildFile buildFileFn
	// BuildEvolvedFile renders a file like BuildFile plus evolvedColumn on every row; the
	// "evolve-schema" operation uploads it.
	BuildEvolvedFile  buildFileFn
	DestinationSchema map[string]string
	// UpdatedDestinationSchema is the destination schema after the "evolve-schema"
	// operation ran; same as DestinationSchema where BuildEvolvedFile is nil.
	UpdatedDestinationSchema map[string]string
	// ExpectedData and ExpectedUpdatedData are the values every synced row must carry. They
	// are per variant because a Parquet file can express types that CSV and JSON cannot.
	ExpectedData        map[string]interface{}
	ExpectedUpdatedData map[string]interface{}
	// WriterExpectedData returns the expected values for the columns whose synced value
	// depends on which destination writer ran (see textWriterExpectedData). Merged into
	// ExpectedData/ExpectedUpdatedData by applyWriterExpectations; nil when every column
	// of the variant syncs identically across destinations.
	WriterExpectedData func(v rowValues, writer s3DestinationWriter) map[string]interface{}
	// ParquetStreaming is the parquet.streaming_enabled value every sync of the variant
	// runs with; meaningful only when DataFormat is "parquet" (see applyParquetStreamingMode).
	ParquetStreaming bool
}

// S3TestVariants lists every source format covered by TestS3Integration.
var S3TestVariants = []S3TestVariant{
	{
		Name:                     "CSV",
		DataFormat:               "csv",
		PlainExt:                 ".csv",
		Gzipped:                  true,
		BuildFile:                buildCSVFile,
		BuildEvolvedFile:         buildEvolvedCSVFile,
		DestinationSchema:        S3CSVToDestinationSchema,
		UpdatedDestinationSchema: S3CSVUpdatedDestinationSchema,
		ExpectedData:             expectedCSVData(seedValues),
		ExpectedUpdatedData:      expectedCSVData(updatedValues),
		WriterExpectedData:       textWriterExpectedData,
	},
	{
		Name:                     "JSON",
		DataFormat:               "json",
		PlainExt:                 ".jsonl",
		Gzipped:                  true,
		BuildFile:                buildJSONLFile,
		BuildEvolvedFile:         buildEvolvedJSONLFile,
		DestinationSchema:        S3JSONToDestinationSchema,
		UpdatedDestinationSchema: S3JSONUpdatedDestinationSchema,
		ExpectedData:             expectedJSONData(seedValues),
		ExpectedUpdatedData:      expectedJSONData(updatedValues),
		WriterExpectedData:       textWriterExpectedData,
	},
	{
		// Identical to Parquet except streaming_enabled=false: every sync loads whole files
		// into memory. Listed first so the suite ends at the committed streaming=true.
		Name:                     "ParquetInMemory",
		DataFormat:               "parquet",
		PlainExt:                 ".parquet",
		BuildFile:                buildParquetFile,
		BuildEvolvedFile:         buildEvolvedParquetFile,
		DestinationSchema:        S3ParquetToDestinationSchema,
		UpdatedDestinationSchema: S3ParquetUpdatedDestinationSchema,
		ExpectedData:             expectedParquetData(seedValues),
		ExpectedUpdatedData:      expectedParquetData(updatedValues),
		WriterExpectedData:       parquetWriterExpectedData,
	},
	{
		// The driver's file matcher recognizes no gzip variant for Parquet, so this stream
		// stays uncompressed.
		Name:                     "Parquet",
		DataFormat:               "parquet",
		PlainExt:                 ".parquet",
		BuildFile:                buildParquetFile,
		BuildEvolvedFile:         buildEvolvedParquetFile,
		DestinationSchema:        S3ParquetToDestinationSchema,
		UpdatedDestinationSchema: S3ParquetUpdatedDestinationSchema,
		ExpectedData:             expectedParquetData(seedValues),
		ExpectedUpdatedData:      expectedParquetData(updatedValues),
		WriterExpectedData:       parquetWriterExpectedData,
		ParquetStreaming:         true,
	},
}

// s3Source is the host-side view of a variant's source.json: the same bucket, credentials
// and prefix the driver uses, reached at 127.0.0.1 rather than the container address.
type s3Source struct {
	client *minio.Client
	bucket string
	prefix string
}

func (v S3TestVariant) source(t *testing.T) s3Source {
	t.Helper()

	config := testutils.ReadSourceConfig(t, filepath.Join("testdata", v.DataFormat, "source.json"))
	endpoint, err := url.Parse(config.String("endpoint"))
	require.NoError(t, err, "failed to parse endpoint")
	client, err := minio.New(net.JoinHostPort("127.0.0.1", endpoint.Port()), &minio.Options{
		Creds: credentials.NewStaticV4(config.String("access_key_id"), config.String("secret_access_key"), ""),
	})
	require.NoError(t, err, "failed to create MinIO client")

	return s3Source{client: client, bucket: config.String("bucket_name"), prefix: config.String("path_prefix")}
}

// removeUnder deletes every object under prefix, tolerating a bucket that does not exist yet
// (a "drop" can run before the first "create").
func (s s3Source) removeUnder(ctx context.Context, t *testing.T, prefix string) {
	t.Helper()
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if minio.ToErrorResponse(obj.Err).Code == "NoSuchBucket" {
			return
		}
		require.NoError(t, obj.Err, "failed to list objects under %s", prefix)
		require.NoError(t, s.client.RemoveObject(ctx, s.bucket, obj.Key, minio.RemoveObjectOptions{}), "failed to remove %s", obj.Key)
	}
}

// s3DestinationWriter identifies the destination writer a sync runs through, as far as
// the variant can tell from its destination config.
type s3DestinationWriter string

const (
	// TODO: arrow and legacy writers differ in timestamp precisions we need to fix the legacy writer to keep micros and then remove this distinction from the test.

	// writerLegacy is the legacy Iceberg writer, which truncates timestamptz to millis.
	writerLegacy s3DestinationWriter = "legacy"
	// writerArrow is the Arrow Iceberg writer, which keeps micros. The Parquet
	// destination also answers to this value: it is not observable from the config (its
	// block just leaves arrow_writes where the Arrow block set it) and it keeps micros
	// exactly like the Arrow writer (types.ToNewParquet pins every timestamp column to
	// parquet.Microsecond), so it never needs telling apart.
	writerArrow s3DestinationWriter = "arrow"
)

// currentDestinationWriter reads the live arrow_writes flag from the destination config the
// next sync will run with, and reports which writer that is.
func (v S3TestVariant) currentDestinationWriter(t *testing.T, config *testutils.TestConfig) s3DestinationWriter {
	t.Helper()

	// The harness picks a writer by swapping IcebergDestinationPath between two files in its
	// private working directory (see testIcebergWriter)
	destPath := filepath.Join(config.HostTestDataPath, filepath.Base(config.IcebergDestinationPath))
	data, err := os.ReadFile(destPath)
	require.NoError(t, err, "failed to read %s", destPath)
	var destConfig struct {
		Writer struct {
			ArrowWrites bool `json:"arrow_writes"`
		} `json:"writer"`
	}
	require.NoError(t, json.Unmarshal(data, &destConfig), "failed to parse %s", destPath)

	if destConfig.Writer.ArrowWrites {
		return writerArrow
	}
	return writerLegacy
}

// applyWriterExpectations retargets the writer-dependent expected values at the writer the
// next sync will use. The harness toggles arrow_writes in the variant's
// iceberg_destination.json before each Iceberg writer block but asserts every block against
// the same ExpectedData maps, so this hook -- the only variant-owned code that runs between
// the toggle and the verification -- reads the live flag and updates the maps in place.
func (v S3TestVariant) applyWriterExpectations(t *testing.T, config *testutils.TestConfig) {
	t.Helper()
	if v.WriterExpectedData == nil {
		return
	}

	writer := v.currentDestinationWriter(t, config)
	maps.Copy(v.ExpectedData, v.WriterExpectedData(seedValues, writer))
	maps.Copy(v.ExpectedUpdatedData, v.WriterExpectedData(updatedValues, writer))
}

// applyParquetStreamingMode pins parquet.streaming_enabled in this config's source.json
func (v S3TestVariant) applyParquetStreamingMode(t *testing.T, config *testutils.TestConfig) {
	t.Helper()
	if v.DataFormat != "parquet" {
		return
	}

	path := config.HostSourcePath
	data, err := os.ReadFile(path)
	require.NoError(t, err, "failed to read %s", path)

	want := fmt.Sprintf(`"streaming_enabled": %t`, v.ParquetStreaming)
	if bytes.Contains(data, []byte(want)) {
		return
	}
	stale := fmt.Sprintf(`"streaming_enabled": %t`, !v.ParquetStreaming)
	require.Contains(t, string(data), stale, "%s lost its streaming_enabled key", path)
	data = bytes.ReplaceAll(data, []byte(stale), []byte(want))
	require.NoError(t, os.WriteFile(path, data, 0o600), "failed to write %s", path)
	t.Logf("parquet source streaming_enabled=%t for upcoming syncs", v.ParquetStreaming)
}

// ExecuteQueryFactory returns the harness ExecuteQuery hook for one S3 format variant.
// Unlike database drivers, operations here manage files in the shared source bucket under
// the variant's path prefix: "create" ensures the bucket exists, "add" seeds the stream,
// "insert"/"update" upload a further file each, and "clean"/"drop" remove everything under
// the prefix.
func ExecuteQueryFactory(variant S3TestVariant) func(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
	return func(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
		t.Helper()

		// Every destination block starts by re-seeding the source through this hook, so
		// refreshing the expectations here keeps them aligned with whichever writer the
		// harness toggled the destination to since the last operation.
		variant.applyWriterExpectations(t, conf)
		variant.applyParquetStreamingMode(t, conf)

		src := variant.source(t)
		prefix := src.prefix + "/" + testutils.TestTableName(conf) + "/"

		switch operation {
		case "drop-all":
			// Everything under the variant's path prefix, not just this stream's folder:
			// discover enumerates one stream per folder, so a folder an aborted run left
			// behind would show up as an extra stream. Safe only in the serial discover
			// suite -- variants sharing a DataFormat share this prefix (see TestDiscover).
			src.removeUnder(ctx, t, src.prefix+"/")

		case "create":
			if err := src.client.MakeBucket(ctx, src.bucket, minio.MakeBucketOptions{}); err != nil {
				code := minio.ToErrorResponse(err).Code
				if code != "BucketAlreadyOwnedByYou" && code != "BucketAlreadyExists" {
					require.NoError(t, err, "failed to create bucket %s", src.bucket)
				}
			}

		case "clean", "drop":
			src.removeUnder(ctx, t, prefix)

		case "add":
			// One plain file and, where the format allows it, one gzipped: a single stream
			// mixing both proves compression is detected per file rather than per stream.
			variant.putFile(ctx, t, src, prefix, "seed_1", variant.BuildFile, 1, seedValues, false)
			variant.putFile(ctx, t, src, prefix, "seed_2", variant.BuildFile, 4, seedValues, variant.Gzipped)

		case "insert":
			// A file the incremental cursor has not seen: it is stamped after the previous
			// sync, so only its rows are re-read.
			variant.putFile(ctx, t, src, prefix, "insert_1", variant.BuildFile, 7, seedValues, false)

		case "update":
			// Object stores have no in-place update: changed data arrives as another file.
			variant.putFile(ctx, t, src, prefix, "update_1", variant.BuildFile, 10, updatedValues, false)

		case "evolve-schema":
			// An object store's ALTER TABLE: a file whose rows carry a column discover has
			// never seen. It is stamped after the previous sync like the "update" file, so
			// the update sync reads both and must evolve the destination to fit this one.
			// The rows carry updatedValues because that sync asserts every row against
			// ExpectedUpdatedData; evolvedColumn itself is asserted through the schema, not
			// per row, since the "update" file's rows sync a null there.
			if variant.BuildEvolvedFile != nil {
				variant.putFile(ctx, t, src, prefix, "evolve_1", variant.BuildEvolvedFile, 13, updatedValues, false)
			}

		default:
			t.Fatalf("unsupported operation: %s", operation)
		}
	}
}

// putFile renders one file with build and uploads it under prefix. Gzipped files get a
// ".gz" suffix, which is what both the driver's file matcher and its reader use to detect
// compression.
func (v S3TestVariant) putFile(ctx context.Context, t *testing.T, src s3Source, prefix, name string, build buildFileFn, startID int64, vals rowValues, gzipped bool) {
	t.Helper()

	data := build(t, startID, vals)
	ext := v.PlainExt
	if gzipped {
		data = gzipBytes(t, data)
		ext += ".gz"
	}

	key := prefix + name + ext
	_, err := src.client.PutObject(ctx, src.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	require.NoError(t, err, "failed to upload %s", key)
	t.Logf("Uploaded s3://%s/%s (%d bytes)", src.bucket, key, len(data))
}

// Sub-second timestamp layouts for the text variants. Fixed-width fractions (not the
// trailing-zero-trimming .999 form) so the file always carries the digit count whose
// precision discover is expected to detect.
const (
	tsMilliLayout = "2006-01-02T15:04:05.000Z07:00"
	tsMicroLayout = "2006-01-02T15:04:05.000000Z07:00"
	tsNanoLayout  = "2006-01-02T15:04:05.000000000Z07:00"
)

// mixedValue returns the mixed_col value for one row, as a CSV cell and as a raw JSON
// token. It cycles number, text and boolean by id so every 3-row file carries all three
// shapes and both parsers fall back to String, the shapes' common ancestor.
func mixedValue(id int64) (csvCell, jsonToken string) {
	switch id % 3 {
	case 1:
		return "42", "42"
	case 2:
		return "text", `"text"`
	default:
		return "true", "true"
	}
}

func buildCSVFile(_ *testing.T, startID int64, vals rowValues) []byte {
	return csvFile(startID, vals, false)
}

// buildEvolvedCSVFile is buildCSVFile plus evolvedColumn: a header the discovered schema
// lacks, which the parser must stream through for the destination to evolve.
func buildEvolvedCSVFile(_ *testing.T, startID int64, vals rowValues) []byte {
	return csvFile(startID, vals, true)
}

func csvFile(startID int64, vals rowValues, evolved bool) []byte {
	var b strings.Builder
	b.WriteString("id,str_col,bool_col,float_col,int_col,mixed_col,null_col,date_col,ts_col,ts_milli_col,ts_micro_col,ts_nano_col," + excludedColumn)
	if evolved {
		b.WriteString("," + evolvedColumn)
	}
	b.WriteString("\n")
	for i := int64(0); i < rowsPerFile; i++ {
		id := startID + i
		mixed, _ := mixedValue(id)
		// null_col is the empty cell after mixed_col: CSV cannot omit a column, so an
		// empty value is how the format spells null.
		b.WriteString(fmt.Sprintf("%d,%s,%t,%v,%d,%s,,%s,%s,%s,%s,%s,%s",
			id, vals.Str, vals.Bool, vals.Float, vals.Int64, mixed,
			vals.TS.UTC().Format(time.DateOnly),
			vals.TS.Format(time.RFC3339),
			vals.TSMilli.Format(tsMilliLayout),
			vals.TSMicro.Format(tsMicroLayout),
			vals.TSNano.Format(tsNanoLayout),
			excludedColumnValue))
		if evolved {
			b.WriteString("," + evolvedColumnValue)
		}
		b.WriteString("\n")
	}
	return []byte(b.String())
}

func buildJSONLFile(_ *testing.T, startID int64, vals rowValues) []byte {
	return jsonlFile(startID, vals, false)
}

// buildEvolvedJSONLFile is buildJSONLFile plus evolvedColumn on every record.
func buildEvolvedJSONLFile(_ *testing.T, startID int64, vals rowValues) []byte {
	return jsonlFile(startID, vals, true)
}

func jsonlFile(startID int64, vals rowValues, evolved bool) []byte {
	var b strings.Builder
	for i := int64(0); i < rowsPerFile; i++ {
		id := startID + i
		_, mixed := mixedValue(id)
		fields := []string{
			fmt.Sprintf(`"id": %d`, id),
			fmt.Sprintf(`"str_col": %q`, vals.Str),
			fmt.Sprintf(`"bool_col": %t`, vals.Bool),
			fmt.Sprintf(`"float_col": %v`, vals.Float),
			fmt.Sprintf(`"int_col": %d`, vals.Int64),
			fmt.Sprintf(`"mixed_col": %s`, mixed),
			fmt.Sprintf(`"object_col": %s`, vals.JSON),
			fmt.Sprintf(`"array_col": %s`, mustJSON(vals.List)),
			fmt.Sprintf(`"date_col": %q`, vals.TS.UTC().Format(time.DateOnly)),
			fmt.Sprintf(`"ts_col": %q`, vals.TS.Format(time.RFC3339)),
			fmt.Sprintf(`"ts_milli_col": %q`, vals.TSMilli.Format(tsMilliLayout)),
			fmt.Sprintf(`"ts_micro_col": %q`, vals.TSMicro.Format(tsMicroLayout)),
			fmt.Sprintf(`"ts_nano_col": %q`, vals.TSNano.Format(tsNanoLayout)),
			fmt.Sprintf(`%q: %q`, excludedColumn, excludedColumnValue),
		}
		// optional_col rides only on two of the three rows: a field some records lack
		// must stay typed by the records that carry it and sync as null elsewhere.
		if id%3 != 0 {
			fields = append(fields, fmt.Sprintf(`"optional_col": %q`, vals.Str))
		}
		if evolved {
			fields = append(fields, fmt.Sprintf(`"%s": %q`, evolvedColumn, evolvedColumnValue))
		}
		b.WriteString("{")
		b.WriteString(strings.Join(fields, ", "))
		b.WriteString("}\n")
	}
	return []byte(b.String())
}

// parquetRow carries one row of the Parquet variant's source file. Unlike the CSV and JSON
// variants, whose parsers infer a handful of types from text, Parquet files carry their own
// schema, so this stream exercises every type the parser maps (see mapParquetTypeToOlake in
// pkg/parser/parquet.go) rather than the small shared set.
//
// The map and nested struct columns reach the destination as one string column each: the
// destination flattener (utils/typeutils/flatten.go) JSON-encodes every non-scalar before
// any writer sees it, so all destinations render them identically and both are pinned to
// exact compact JSON.
type parquetRow struct {
	ID int64 `parquet:"id"`

	BoolCol bool `parquet:"bool_col"`

	Int8Col  int32 `parquet:"int8_col"`
	Int16Col int32 `parquet:"int16_col"`
	Int32Col int32 `parquet:"int32_col"`
	Int64Col int64 `parquet:"int64_col"`

	Uint8Col  int32 `parquet:"uint8_col"`
	Uint16Col int32 `parquet:"uint16_col"`
	Uint32Col int32 `parquet:"uint32_col"`
	Uint64Col int64 `parquet:"uint64_col"`

	Float32Col float32 `parquet:"float32_col"`
	FloatCol   float64 `parquet:"float_col"`

	StrCol     string   `parquet:"str_col"`
	UnicodeCol string   `parquet:"unicode_col"`
	EmptyCol   string   `parquet:"empty_col"`
	NullCol    *string  `parquet:"null_col"`
	BytesCol   []byte   `parquet:"bytes_col"`
	JSONCol    string   `parquet:"json_col"`
	EnumCol    string   `parquet:"enum_col"`
	UUIDCol    [16]byte `parquet:"uuid_col"`
	Dec32Col   int32    `parquet:"dec32_col"`
	Dec64Col   int64    `parquet:"dec64_col"`
	// The byte-array physical form decimals take above 18 digits: the unscaled integer as
	// big-endian two's complement, the encoding the parser has to sign-extend itself.
	DecBytesCol [16]byte `parquet:"dec_bytes_col"`

	DateCol   int32 `parquet:"date_col"`
	TimeMsCol int32 `parquet:"time_ms_col"`
	TimeUsCol int64 `parquet:"time_us_col"`
	TimeNsCol int64 `parquet:"time_ns_col"`
	TSMsCol   int64 `parquet:"ts_ms_col"`
	TSCol     int64 `parquet:"ts_col"`
	TSNsCol   int64 `parquet:"ts_ns_col"`
	// TSFarCol carries the far-future instant scaling bugs wrap: micros scaled up into
	// int64 nanoseconds overflow past ~2262 and used to come back as year 1816.
	TSFarCol    int64            `parquet:"ts_far_col"`
	Int96Col    deprecated.Int96 `parquet:"int96_col"`
	ExcludedCol string           `parquet:"excluded_col"`

	MapCol    map[string]string `parquet:"map_col"`
	StructCol struct {
		A string `parquet:"a"`
		B int64  `parquet:"b"`
	} `parquet:"struct_col"`

	// A plain slice rather than the three level LIST structure List() describes: rows are
	// deconstructed by the schema (see writeParquetRows), and Deconstruct collapses a LIST
	// node to its repeated element, so list_col binds to the slice itself.
	ListCol []string `parquet:"list_col"`

	// NewCol carries evolvedColumn, which only evolvedParquetTestSchema declares. Rows are
	// deconstructed against a schema, so a field no column names is never looked up: the
	// base file ignores this one, and the evolved file picks it up. It lives here rather
	// than on a wrapper type embedding parquetRow because field lookup does not descend
	// into embedded structs, which would leave every promoted column empty.
	NewCol string `parquet:"new_col"`
}

// parquetTestGroup pins the parquet type of every column explicitly instead of letting
// parquet-go infer it from the Go types above. Inference cannot express the full matrix:
// it has no unsigned or 8/16 bit integer kinds, and its "uuid" struct tag only validates
// that the field is a [16]byte without annotating the column as a UUID. A fresh group per
// call so the evolved schema can extend it without mutating the base.
func parquetTestGroup() pq.Group {
	return pq.Group{
		"id": pq.Int(64),

		"bool_col": pq.Leaf(pq.BooleanType),

		"int8_col":  pq.Int(8),
		"int16_col": pq.Int(16),
		"int32_col": pq.Int(32),
		"int64_col": pq.Int(64),

		"uint8_col":  pq.Uint(8),
		"uint16_col": pq.Uint(16),
		"uint32_col": pq.Uint(32),
		"uint64_col": pq.Uint(64),

		"float32_col": pq.Leaf(pq.FloatType),
		"float_col":   pq.Leaf(pq.DoubleType),

		"str_col":       pq.String(),
		"unicode_col":   pq.String(),
		"empty_col":     pq.String(),
		"null_col":      pq.Optional(pq.String()),
		"bytes_col":     pq.Leaf(pq.ByteArrayType),
		"json_col":      pq.JSON(),
		"enum_col":      pq.Enum(),
		"uuid_col":      pq.UUID(),
		"dec32_col":     pq.Decimal(2, 9, pq.Int32Type),
		"dec64_col":     pq.Decimal(4, 18, pq.Int64Type),
		"dec_bytes_col": pq.Decimal(2, 38, pq.FixedLenByteArrayType(16)),

		"date_col":     pq.Date(),
		"time_ms_col":  pq.Time(pq.Millisecond),
		"time_us_col":  pq.Time(pq.Microsecond),
		"time_ns_col":  pq.Time(pq.Nanosecond),
		"ts_ms_col":    pq.Timestamp(pq.Millisecond),
		"ts_col":       pq.Timestamp(pq.Microsecond),
		"ts_ns_col":    pq.Timestamp(pq.Nanosecond),
		"ts_far_col":   pq.Timestamp(pq.Microsecond),
		"int96_col":    pq.Leaf(pq.Int96Type),
		"excluded_col": pq.String(),

		"map_col":    pq.Map(pq.String(), pq.String()),
		"struct_col": pq.Group{"a": pq.String(), "b": pq.Int(64)},
		"list_col":   pq.List(pq.String()),
	}
}

var (
	parquetTestSchema = pq.NewSchema("s3_parquet_row", parquetTestGroup())

	// evolvedParquetTestSchema is the base schema plus evolvedColumn, for the file the
	// "evolve-schema" operation uploads.
	evolvedParquetTestSchema = pq.NewSchema("s3_parquet_row", func() pq.Group {
		group := parquetTestGroup()
		group[evolvedColumn] = pq.String()
		return group
	}())
)

func makeParquetRow(id int64, vals rowValues) parquetRow {
	row := parquetRow{
		ID: id,

		BoolCol: vals.Bool,

		Int8Col:  int32(vals.Int8),
		Int16Col: int32(vals.Int16),
		Int32Col: vals.Int32,
		Int64Col: vals.Int64,

		//nolint:gosec // G115: storing the unsigned bit pattern in the signed physical type is the point
		Uint8Col: int32(vals.Uint8),
		//nolint:gosec // G115: as above
		Uint16Col: int32(vals.Uint16),
		//nolint:gosec // G115: as above
		Uint32Col: int32(vals.Uint32),
		//nolint:gosec // G115: as above
		Uint64Col: int64(vals.Uint64),

		Float32Col: vals.Float32,
		FloatCol:   vals.Float,

		StrCol:      vals.Str,
		UnicodeCol:  vals.Unicode,
		EmptyCol:    "",
		NullCol:     nil,
		BytesCol:    vals.Bytes,
		JSONCol:     vals.JSON,
		EnumCol:     vals.Enum,
		UUIDCol:     vals.UUID,
		Dec32Col:    vals.Dec32,
		Dec64Col:    vals.Dec64,
		DecBytesCol: decimalToFixedBytes(vals.Dec32),

		//nolint:gosec // G115: fixed seed values, well inside int32
		DateCol: int32(vals.TS.Truncate(24*time.Hour).Unix() / 86400),
		//nolint:gosec // G115: fixed seed values, well inside int32
		TimeMsCol: int32(vals.TimeOfDay.Milliseconds()),
		TimeUsCol: vals.TimeOfDay.Microseconds(),
		TimeNsCol: vals.TimeOfDay.Nanoseconds(),
		// Each timestamp column carries the seed of its own precision, so the file holds
		// real sub-second digits for the parser to keep and the writers to floor.
		TSMsCol:     vals.TSMilli.UnixMilli(),
		TSCol:       vals.TSMicro.UnixMicro(),
		TSNsCol:     vals.TSNano.UnixNano(),
		TSFarCol:    farFutureTS.UnixMicro(),
		Int96Col:    timeToInt96(vals.TSNano),
		ExcludedCol: excludedColumnValue,

		MapCol:  map[string]string{"k": vals.Str},
		ListCol: vals.List,
	}
	row.StructCol.A = vals.Str
	row.StructCol.B = vals.Int64
	return row
}

func buildParquetFile(t *testing.T, startID int64, vals rowValues) []byte {
	t.Helper()
	rows := make([]parquetRow, 0, rowsPerFile)
	for i := int64(0); i < rowsPerFile; i++ {
		rows = append(rows, makeParquetRow(startID+i, vals))
	}

	return writeParquetRows(t, parquetTestSchema, rows)
}

func writeParquetRows(t *testing.T, schema *pq.Schema, rows []parquetRow) []byte {
	t.Helper()

	deconstructed := make([]pq.Row, 0, len(rows))
	for i := range rows {
		deconstructed = append(deconstructed, schema.Deconstruct(nil, &rows[i]))
	}

	var buf bytes.Buffer
	writer := pq.NewGenericWriter[parquetRow](&buf, schema)
	_, err := writer.WriteRows(deconstructed)
	require.NoError(t, err, "failed to write parquet rows")
	require.NoError(t, writer.Close(), "failed to close parquet writer")
	return buf.Bytes()
}

// buildEvolvedParquetFile is buildParquetFile plus evolvedColumn on every row: a column
// the discovered schema lacks, which the parser hands through for the destination to evolve.
func buildEvolvedParquetFile(t *testing.T, startID int64, vals rowValues) []byte {
	t.Helper()
	rows := make([]parquetRow, 0, rowsPerFile)
	for i := int64(0); i < rowsPerFile; i++ {
		row := makeParquetRow(startID+i, vals)
		row.NewCol = evolvedColumnValue
		rows = append(rows, row)
	}

	return writeParquetRows(t, evolvedParquetTestSchema, rows)
}

// decimalToFixedBytes renders the unscaled integer as the 16 byte big-endian two's
// complement layout a FIXED_LEN_BYTE_ARRAY decimal column carries.
func decimalToFixedBytes(unscaled int32) [16]byte {
	var out [16]byte
	v := big.NewInt(int64(unscaled))
	if unscaled < 0 {
		v.Add(v, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	v.FillBytes(out[:])
	return out
}

// timeToInt96 renders t in the legacy Impala/Hive Int96 layout the parser decodes: the low
// 8 bytes hold nanoseconds within the day, the high 4 bytes hold the Julian day.
func timeToInt96(t time.Time) deprecated.Int96 {
	day := t.UTC().Truncate(24 * time.Hour)
	nanosOfDay := t.UTC().Sub(day).Nanoseconds()
	julianDay := day.Unix()/86400 + 2440588
	//nolint:gosec // G115: the values are bounded by the day and the epoch offset
	return deprecated.Int96{uint32(nanosOfDay & 0xFFFFFFFF), uint32(nanosOfDay >> 32), uint32(julianDay)}
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(data)
	require.NoError(t, err, "failed to gzip data")
	require.NoError(t, writer.Close(), "failed to close gzip writer")
	return buf.Bytes()
}
