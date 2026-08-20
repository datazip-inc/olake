package destination

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cockroachdb/pebble/v2"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// StateFileName is the file used to persist the last inserted olake ID (one integer per line or single line).
const StateFileName = "state.txt"

// Dummy column names for 8-column dummy data.
const (
	col1, col2, col3, col4 = "name", "age", "score", "active"
	col5, col6, col7, col8 = "email", "code", "count", "category"
)

func readLastOlakeID() (int, error) {
	b, err := os.ReadFile(StateFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var last int
	_, _ = fmt.Sscanf(string(b), "%d", &last)
	return last, nil
}

func writeLastOlakeID(last int) error {
	return os.WriteFile(StateFileName, []byte(strconv.Itoa(last)+"\n"), 0644)
}

func makeDummyData(seed int) map[string]any {
	return map[string]any{
		col1: fmt.Sprintf("user_%d", seed),
		col2: seed % 100,
		col3: float64(seed) * 1.5,
		col4: seed%2 == 0,
		col5: fmt.Sprintf("user%d@example.com", seed),
		col6: fmt.Sprintf("CODE-%04d", seed),
		col7: seed * 10,
		col8: fmt.Sprintf("cat_%d", seed%5),
	}
}

// getWriterConfig centralizes the ICEBERG config setup for tests.
func getWriterConfig(s3Path string) *types.WriterConfig {
	return &types.WriterConfig{
		Type: types.Iceberg,
		WriterConfig: map[string]any{
			"catalog_type":         "jdbc",
			"jdbc_url":             "jdbc:postgresql://localhost:5432/iceberg",
			"jdbc_username":        "iceberg",
			"jdbc_password":        "password",
			"iceberg_s3_path":      s3Path,
			"s3_endpoint":          "http://localhost:9000",
			"s3_use_ssl":           false,
			"s3_path_style":        true,
			"aws_region":           "us-east-1",
			"aws_access_key":       "admin",
			"aws_secret_key":       "password",
			"no_identifier_fields": false,
			"arrow_writes":         true,
		},
	}
}

// getStream sets up the schema for the dummy table, with optional column 8.
func getStream(suffix int, addPos bool, addCol8 bool) *types.ConfiguredStream {
	stream := &types.ConfiguredStream{
		StreamMetadata: types.StreamMetadata{
			StreamName:    "testing",
			Normalization: true,
			DeleteType:    utils.Ternary(addPos, string(types.DeleteModePosition), string(types.DeleteModeEquality)).(string),
		},
		Stream: &types.Stream{
			Name:                    fmt.Sprintf("auto_table_%d", suffix),
			Namespace:               "amoro_kind",
			SourceDefinedPrimaryKey: types.NewSet(constants.OlakeID),
			Schema:                  &types.TypeSchema{Properties: sync.Map{}},
			DestinationDatabase:     "io_test",
		},
	}
	stream.Stream.Schema.AddTypes(constants.OlakeID, true, types.String)
	stream.Stream.Schema.AddTypes(constants.OlakeTimestamp, true, types.Timestamp)
	stream.Stream.Schema.AddTypes(constants.OpType, true, types.String)

	stream.Stream.Schema.AddTypes(col1, false, types.String)
	stream.Stream.Schema.AddTypes(col2, false, types.Int64)
	stream.Stream.Schema.AddTypes(col3, false, types.Float64)
	stream.Stream.Schema.AddTypes(col4, false, types.Bool)
	stream.Stream.Schema.AddTypes(col5, false, types.String)
	stream.Stream.Schema.AddTypes(col6, false, types.String)
	stream.Stream.Schema.AddTypes(col7, false, types.Int64)
	if addCol8 {
		stream.Stream.Schema.AddTypes(col8, false, types.String)
	}
	return stream
}

// getRecords generates raw records to flush.
func getRecords(operation string, startID, endID int, includeCol8 bool) []types.RawRecord {
	buf := make([]types.RawRecord, 0, endID-startID+1)
	for id := startID; id <= endID; id++ {
		data := makeDummyData(id)
		if !includeCol8 {
			delete(data, col8)
		}
		OlakeColumns := map[string]any{
			constants.OlakeID:        strconv.Itoa(id),
			constants.OlakeTimestamp: time.Now().UTC(),
			constants.OpType:         operation,
		}
		buf = append(buf, types.CreateRawRecord(data, OlakeColumns))
	}
	return buf
}

// runWriterThread simplifies creating a writer thread and flushing records to it.
func runWriterThread(ctx context.Context, config *types.WriterConfig, stream *types.ConfiguredStream, records []types.RawRecord) error {
	pool, err := NewWriterPool(ctx, config, []types.StreamInterface{stream}, 1000)
	if err != nil {
		return fmt.Errorf("failed in writer pool: %w", err)
	}
	defer pool.Shutdown(ctx)

	newWriterThread, _, err := pool.NewWriter(ctx, stream, WithThreadID("iceberg_script_test"))
	if err != nil {
		return fmt.Errorf("failed in creating thread: %w", err)
	}

	if err := newWriterThread.flush(ctx, records); err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

	if err := newWriterThread.Close(ctx, nil); err != nil {
		return err
	}
	return nil
}

// getTestRange checks operation conditions and determines ID ranges based on current state.
func getTestRange(operation string, numRecords int) (int, int, error) {
	if numRecords <= 0 {
		return 0, 0, fmt.Errorf("numRecords must be positive, got %d", numRecords)
	}

	lastID, err := readLastOlakeID()
	if err != nil {
		return 0, 0, fmt.Errorf("read state: %w", err)
	}

	var startID, endID int
	switch operation {
	case "c":
		startID = lastID + 1
		endID = lastID + numRecords
	case "u":
		// if numRecords > lastID {
		// 	return 0, 0, fmt.Errorf("cannot update %d records: only %d olake IDs exist (state.txt has last_id=%d). Insert first", numRecords, lastID, lastID)
		// }
		startID = 1
		endID = numRecords
	case "d":
		startID = 1
		endID = numRecords
	}
	return startID, endID, nil
}

func WriteData(opType string, suffix int, operation string, numRecords int) error {
	switch opType {
	case "schema":
		return SchemaEvolve(suffix, true, operation, numRecords)
	case "partition":
		return WriteDataWithPartition(suffix, true, operation, numRecords)
	case "eqtopos":
		if err := Equality(suffix, operation, numRecords); err != nil {
			return err
		}
		return WriteDataDef(suffix, true, operation, numRecords)
	case "eq":
		return Equality(suffix, operation, numRecords)
	case "conflict":
		if err := WriteDataDef(suffix, true, operation, 3); err != nil {
			return err
		}
		// drop table only
		if err := DropTable(suffix, false); err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
		// check index exist
		if err := ReadIndex(suffix); err != nil {
			return fmt.Errorf("failed to read index: %w", err)
		}

		// update 2 records
		return WriteDataDef(suffix, true, "u", 2)
	case "pos":
		return WriteDataDef(suffix, true, operation, numRecords)

	default:
		return WriteDataDef(suffix, true, operation, numRecords)
	}
}

// WriteDataDef runs an insert/update with DeleteModePosition
func WriteDataDef(suffix int, pos bool, operation string, numRecords int) error {
	startID, endID, err := getTestRange(operation, numRecords)
	if err != nil {
		return err
	}

	wtConfig := getWriterConfig("s3a://warehouse/io")
	stream := getStream(suffix, pos, true)

	records := getRecords(operation, startID, endID, true)
	if err := runWriterThread(context.TODO(), wtConfig, stream, records); err != nil {
		return err
	}

	if operation == "c" {
		return writeLastOlakeID(endID)
	}
	return nil
}

// WriteDataWithPartition runs an insert/update with partition
func WriteDataWithPartition(suffix int, pos bool, operation string, numRecords int) error {
	startID, endID, err := getTestRange(operation, numRecords)
	if err != nil {
		return err
	}

	wtConfig := getWriterConfig("s3a://warehouse/io")
	stream := getStream(suffix, pos, true)

	stream.StreamMetadata.PartitionRegex = "/{name, identity}"
	records := getRecords(operation, startID, endID, true)
	if err := runWriterThread(context.TODO(), wtConfig, stream, records); err != nil {
		return err
	}

	if operation == "c" {
		return writeLastOlakeID(endID)
	}
	return nil
}

// Equality runs an insert/update with DeleteModeEquality
func Equality(suffix int, operation string, numRecords int) error {
	startID, endID, err := getTestRange(operation, numRecords)
	if err != nil {
		return err
	}

	wtConfig := getWriterConfig("s3a://warehouse/io")
	stream := getStream(suffix, false, true)

	records := getRecords(operation, startID, endID, false)
	if err := runWriterThread(context.TODO(), wtConfig, stream, records); err != nil {
		return err
	}

	if operation == "c" {
		return writeLastOlakeID(endID)
	}
	return nil
}

// SchemaEvolve tests flushing records when the schema evolves mid-session
func SchemaEvolve(suffix int, pos bool, operation string, numRecords int) error {
	startID, endID, err := getTestRange(operation, numRecords)
	if err != nil {
		return err
	}

	wtConfig := getWriterConfig("s3a://warehouse/io")
	stream := getStream(suffix, pos, false) // Start without col8

	records := getRecords(operation, startID, endID, false)

	pool, err := NewWriterPool(context.TODO(), wtConfig, []types.StreamInterface{stream}, 1000)
	if err != nil {
		return fmt.Errorf("failed in writer pool: %w", err)
	}
	defer pool.Shutdown(context.TODO())

	newWriterThread, _, err := pool.NewWriter(context.TODO(), stream, WithThreadID("iceberg_script_test"))
	if err != nil {
		return fmt.Errorf("failed in creating thread: %w", err)
	}

	if err := newWriterThread.flush(context.TODO(), records); err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

	// Evolve schema and flush next batch
	startID += numRecords
	endID += numRecords

	stream = getStream(suffix, pos, true)
	records2 := getRecords(operation, startID, endID, true)

	records2[0].OlakeColumns[constants.OlakeID] = "1"
	records2[1].OlakeColumns[constants.OlakeID] = "2"
	records2[0].OlakeColumns[constants.OpType] = "u"
	records2[1].OlakeColumns[constants.OpType] = "u"
	records2[0].Data[col8] = "this record duplicate with 1"
	records2[1].Data[col8] = "this record duplicate with 2"

	if err := newWriterThread.flush(context.TODO(), records2); err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

	if err := newWriterThread.Close(context.TODO(), nil); err != nil {
		return err
	}

	if operation == "c" {
		return writeLastOlakeID(endID)
	}
	return nil
}

func deleteS3FilesForTable(ctx context.Context, suffix int) error {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "password", "")),
	)
	if err != nil {
		return err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:9000")
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	bucket := "warehouse"
	tableName := fmt.Sprintf("auto_table_%d", suffix)

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("io"),
	})

	var objectsToDelete []s3types.ObjectIdentifier
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			logger.Warnf("could not list S3 objects for cleanup: %v", err)
			return nil
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && strings.Contains(*obj.Key, tableName) {
				objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
					Key: obj.Key,
				})
			}
		}
	}

	if len(objectsToDelete) > 0 {
		_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete S3 objects for table %s: %w", tableName, err)
		}
		logger.Infof("Deleted %d S3 objects from bucket %s for table %s", len(objectsToDelete), bucket, tableName)
	}

	return nil
}

func DropTable(suffix int, pos bool) error {
	wtConfig := getWriterConfig("s3a://warehouse/io")
	stream := getStream(suffix, pos, true)
	_ = os.Remove(StateFileName)
	err := DropStreams(context.TODO(), wtConfig, []types.StreamInterface{stream})
	if err != nil {
		return fmt.Errorf("failed to drop streams: %w", err)
	}
	if err := deleteS3FilesForTable(context.TODO(), suffix); err != nil {
		logger.Warnf("failed to delete S3 files for table suffix %d: %v", suffix, err)
	}
	return nil
}

type nopPebbleLogger struct{}

func (nopPebbleLogger) Infof(format string, args ...interface{})  {}
func (nopPebbleLogger) Errorf(format string, args ...interface{}) {}
func (nopPebbleLogger) Fatalf(format string, args ...interface{}) {}

func ReadIndex(suffix int) error {
	stream := getStream(suffix, true, true)
	streamID := stream.Stream.ID()

	patterns := []string{
		fmt.Sprintf("%s/%s/*/*/MANIFEST-*", constants.DefaultDirName, streamID),
		fmt.Sprintf("%s/%s/*/MANIFEST-*", constants.DefaultDirName, streamID),
		fmt.Sprintf("main/%s/%s/*/*/MANIFEST-*", constants.DefaultDirName, streamID),
		fmt.Sprintf("main/%s/%s/*/MANIFEST-*", constants.DefaultDirName, streamID),
		fmt.Sprintf("%s/*auto_table_%d*/*/MANIFEST-*", constants.DefaultDirName, suffix),
		fmt.Sprintf("%s/*auto_table_%d*/MANIFEST-*", constants.DefaultDirName, suffix),
	}

	var allMatches []string
	for _, p := range patterns {
		matches, err := filepath.Glob(p)
		if err == nil && len(matches) > 0 {
			allMatches = append(allMatches, matches...)
		}
	}

	if len(allMatches) == 0 {
		return fmt.Errorf("could not find index directory containing Pebble MANIFEST for stream [%s] (suffix %d)", streamID, suffix)
	}

	var latestManifest string
	var latestModTime time.Time
	for _, m := range allMatches {
		info, err := os.Stat(m)
		if err == nil {
			if latestManifest == "" || info.ModTime().After(latestModTime) {
				latestModTime = info.ModTime()
				latestManifest = m
			}
		}
	}

	dir := filepath.Dir(latestManifest)
	logger.Infof("Reading pebble db at %s", dir)

	db, err := pebble.Open(dir, &pebble.Options{
		ReadOnly: true,
		Logger:   nopPebbleLogger{},
	})
	if err != nil {
		return fmt.Errorf("failed to open pebble db: %w", err)
	}
	defer db.Close()

	// 0. Read indexed snapshot ID (prefixMeta = 0x04)
	snapshotKey := append([]byte{0x04}, []byte("snapshot")...)
	if val, closer, err := db.Get(snapshotKey); err == nil {
		snapshotID, read := binary.Varint(val)
		if read > 0 {
			fmt.Printf("Current Snapshot ID: %d\n", snapshotID)
		} else {
			fmt.Println("Current Snapshot ID: invalid varint")
		}
		_ = closer.Close()
	} else if errors.Is(err, pebble.ErrNotFound) {
		fmt.Println("Current Snapshot ID: none (not set)")
	} else {
		fmt.Printf("Failed to read snapshot ID: %v\n", err)
	}

	// 1. Read file ID to path mappings (prefixIDToFilePath = 0x02)
	filePaths := make(map[uint64]string)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0x02},
		UpperBound: []byte{0x03},
	})
	if err != nil {
		return fmt.Errorf("failed to create iter for files: %w", err)
	}
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) == 9 && key[0] == 0x02 {
			id := binary.BigEndian.Uint64(key[1:])
			filePaths[id] = string(iter.Value())
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	// 2. Read all row indices (prefixRow = 0x01)
	iter, err = db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0x01},
		UpperBound: []byte{0x02},
	})
	if err != nil {
		return fmt.Errorf("failed to create iter for rows: %w", err)
	}
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if len(key) > 1 && key[0] == 0x01 {
			rowID := string(key[1:])
			fileID, read := binary.Uvarint(val)
			if read <= 0 {
				continue
			}
			rawPos, readPos := binary.Uvarint(val[read:])
			if readPos <= 0 || rawPos > math.MaxInt64 {
				continue
			}
			pos := int64(rawPos)

			path := filePaths[fileID]
			if path == "" {
				path = fmt.Sprintf("unknown_file_%d", fileID)
			}

			fmt.Printf("olake_id: %s, file_id: %d, pos: %d (path: %s)\n", rowID, fileID, pos, path)
			count++
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	logger.Infof("Total %d indexes printed for table suffix %d", count, suffix)
	return nil
}
