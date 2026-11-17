package testutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

const ParquetTestBucket = "warehouse"

// NewMinIOClient returns a client for the MinIO instance backing the parquet destination in tests.
func NewMinIOClient() (*minio.Client, error) {
	client, err := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("admin", "password", ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %s", err)
	}
	return client, nil
}

// ListParquetObjects lists the .parquet objects lying directly in a table's folder in MinIO.
func ListParquetObjects(ctx context.Context, client *minio.Client, parquetDB, tableName string) ([]minio.ObjectInfo, error) {
	objects := []minio.ObjectInfo{}
	for object := range client.ListObjects(ctx, ParquetTestBucket, minio.ListObjectsOptions{
		Prefix:    parquetTablePath(parquetDB, tableName),
		Recursive: false,
	}) {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %s", object.Err)
		}
		if strings.HasSuffix(object.Key, ".parquet") {
			objects = append(objects, object)
		}
	}
	return objects, nil
}

// ParquetColumnKinds reads the footer of every parquet file directly in a table's folder and
// returns, per file, each column's physical type: BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY(16), INT64
// and so on. Spark reports every byte column as binary, so a fixed width can only be asserted
// here.
func ParquetColumnKinds(ctx context.Context, client *minio.Client, parquetDB, tableName string) (map[string]map[string]string, error) {
	objects, err := ListParquetObjects(ctx, client, parquetDB, tableName)
	if err != nil {
		return nil, err
	}
	kinds := make(map[string]map[string]string, len(objects))
	for _, object := range objects {
		data, err := ReadParquetTableObject(ctx, client, parquetDB, tableName, strings.TrimPrefix(object.Key, parquetTablePath(parquetDB, tableName)))
		if err != nil {
			return nil, err
		}
		file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %s", object.Key, err)
		}
		columns := map[string]string{}
		for _, path := range file.Schema().Columns() {
			leaf, _ := file.Schema().Lookup(path...)
			columns[strings.Join(path, ".")] = parquetKind(leaf.Node)
		}
		kinds[object.Key] = columns
	}
	return kinds, nil
}

// parquetKind is a leaf column's physical type, with the width of a FIXED_LEN_BYTE_ARRAY.
func parquetKind(node parquet.Node) string {
	kind := node.Type().Kind().String()
	if node.Type().Kind() == parquet.FixedLenByteArray {
		kind = fmt.Sprintf("%s(%d)", kind, node.Type().Length())
	}
	return kind
}

// parquetTablePath is the MinIO key prefix a stream's parquet files are written under.
func parquetTablePath(parquetDB, tableName string) string {
	return fmt.Sprintf("%s/%s/", parquetDB, tableName)
}

// The parquet writer's 2PC layout, relative to the table folder (destination/parquet/parquet_2pc.go).
const (
	parquetMetadataKey   = "metadata.json"
	parquetStagingPrefix = "_olake_2pc/"
)

// CreateParquetView stands a temp view over a table's parquet files, dropped when the test ends. The
// error is returned as is: PATH_NOT_FOUND means no files were written, which some callers expect.
func CreateParquetView(ctx context.Context, t *testing.T, spark sql.SparkSession, view, parquetDB, tableName string) error {
	t.Helper()
	path := fmt.Sprintf("s3a://%s/%s*.parquet", ParquetTestBucket, parquetTablePath(parquetDB, tableName))
	if _, err := spark.Sql(ctx, fmt.Sprintf("CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM parquet.`%s`", view, path)); err != nil {
		return err
	}
	// t.Context() is canceled before cleanups run, so the drop needs a context that outlives it.
	dropCtx := context.WithoutCancel(ctx)
	t.Cleanup(func() { _, _ = spark.Sql(dropCtx, "DROP VIEW IF EXISTS "+view) })
	return nil
}

// DeleteParquetFiles deletes only .parquet files directly in the table folder in MinIO
func DeleteParquetFiles(t *testing.T, parquetDB, tableName string) error {
	t.Helper()
	parquetPath := parquetTablePath(parquetDB, tableName)

	t.Logf("Cleaning up .parquet files in: s3a://%s/%s", ParquetTestBucket, parquetPath)

	minioClient, err := NewMinIOClient()
	if err != nil {
		return err
	}

	ctx := t.Context()

	objects, err := ListParquetObjects(ctx, minioClient, parquetDB, tableName)
	if err != nil {
		return err
	}

	for _, object := range objects {
		t.Logf("Deleting: %s", strings.TrimPrefix(object.Key, parquetPath))

		if err := minioClient.RemoveObject(ctx, ParquetTestBucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to delete %s: %s", object.Key, err)
		}
	}

	t.Logf("--- Cleanup Complete: Deleted %d files ---", len(objects))
	return nil
}

// DeleteParquetTable wipes a table's prefix recursively, unlike DeleteParquetFiles: it takes the
// destination metadata with it, so the next sync starts as a genuinely initial one.
func DeleteParquetTable(t *testing.T, parquetDB, tableName string) error {
	t.Helper()
	parquetPath := parquetTablePath(parquetDB, tableName)

	minioClient, err := NewMinIOClient()
	if err != nil {
		return err
	}

	ctx := context.Background()
	deletedCount := 0
	for object := range minioClient.ListObjects(ctx, ParquetTestBucket, minio.ListObjectsOptions{
		Prefix:    parquetPath,
		Recursive: true,
	}) {
		if object.Err != nil {
			return fmt.Errorf("error listing objects: %s", object.Err)
		}
		if err := minioClient.RemoveObject(ctx, ParquetTestBucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to delete %s: %s", object.Key, err)
		}
		deletedCount++
	}

	t.Logf("--- Parquet Table Cleanup Complete: Deleted %d objects ---", deletedCount)
	return nil
}

// ParquetTableObjects lists every object under a table's folder in MinIO, recursively: the data
// files in the table folder and its partition folders, metadata.json and any _olake_2pc staging.
// Keys are returned relative to the table folder.
func ParquetTableObjects(ctx context.Context, client *minio.Client, parquetDB, tableName string) ([]string, error) {
	prefix := parquetTablePath(parquetDB, tableName)
	var keys []string
	for object := range client.ListObjects(ctx, ParquetTestBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %s", object.Err)
		}
		keys = append(keys, strings.TrimPrefix(object.Key, prefix))
	}
	return keys, nil
}

// ReadParquetTableObject reads one object of a table's folder, by its key relative to that folder.
func ReadParquetTableObject(ctx context.Context, client *minio.Client, parquetDB, tableName, key string) ([]byte, error) {
	reader, err := client.GetObject(ctx, ParquetTestBucket, parquetTablePath(parquetDB, tableName)+key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %s", key, err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %s", key, err)
	}
	return data, nil
}

// parquetObject is one object of a parquet table's folder, by its key relative to that folder.
type parquetObject struct {
	key  string
	data []byte
}

// ReadParquetMetadata reads and normalizes a parquet table folder: metadata.json, what is left under
// _olake_2pc, and the footer of every data file.
func ReadParquetMetadata(ctx context.Context, t *testing.T, parquetDB, tableName string) DestinationMetadata {
	t.Helper()
	client, err := NewMinIOClient()
	require.NoError(t, err, "failed to create the MinIO client")
	keys, err := ParquetTableObjects(ctx, client, parquetDB, tableName)
	require.NoErrorf(t, err, "failed to list the objects of %s/%s", parquetDB, tableName)

	objects := make([]parquetObject, 0, len(keys))
	for _, key := range keys {
		object := parquetObject{key: key}
		// Staging is counted, not read: after a committed sync nothing should be left there.
		if key == parquetMetadataKey || (strings.HasSuffix(key, ".parquet") && !strings.HasPrefix(key, parquetStagingPrefix)) {
			object.data, err = ReadParquetTableObject(ctx, client, parquetDB, tableName, key)
			require.NoErrorf(t, err, "failed to read %s/%s/%s", parquetDB, tableName, key)
		}
		objects = append(objects, object)
	}
	metadata, err := normalizeParquetMetadata(objects)
	require.NoErrorf(t, err, "failed to normalize the metadata of %s/%s", parquetDB, tableName)
	return metadata
}

// normalizeParquetMetadata is the pure half of ReadParquetMetadata, kept apart so it is unit-testable.
// A data file is grouped by its folder, which is its partition.
func normalizeParquetMetadata(objects []parquetObject) (DestinationMetadata, error) {
	m := newDestinationMetadata()
	m.set(MetaStaging, "objects", "0")

	schemas := map[string]map[string]bool{} // column -> its type in each file
	codecs := map[string]map[string]bool{}  // column -> its codec in each row group
	for _, object := range objects {
		switch {
		case strings.HasPrefix(object.key, parquetStagingPrefix):
			m.add(MetaStaging, "objects", 1)
		case object.key == parquetMetadataKey:
			normalizeOlake2PC(m, string(object.data))
		case strings.HasSuffix(object.key, ".parquet"):
			partition := path.Dir(object.key)
			if partition == "." {
				partition = "(unpartitioned)"
			}
			file, err := parquet.OpenFile(bytes.NewReader(object.data), int64(len(object.data)))
			if err != nil {
				return DestinationMetadata{}, fmt.Errorf("failed to open %s: %s", object.key, err)
			}
			m.addPartition(MetaFiles, partition, "data %p", 1)
			m.addPartition(MetaDataRecords, partition, "%p", file.NumRows())

			for _, columnPath := range file.Schema().Columns() {
				leaf, _ := file.Schema().Lookup(columnPath...)
				column := strings.Join(columnPath, ".")
				if schemas[column] == nil {
					schemas[column] = map[string]bool{}
				}
				schemas[column][parquetColumnType(leaf.Node)] = true
			}
			for _, rowGroup := range file.Metadata().RowGroups {
				for _, chunk := range rowGroup.Columns {
					column := strings.Join(chunk.MetaData.PathInSchema, ".")
					m.add(MetaColumnStats, column+".null_count", chunk.MetaData.Statistics.NullCount)
					if codecs[column] == nil {
						codecs[column] = map[string]bool{}
					}
					codecs[column][chunk.MetaData.Codec.String()] = true
				}
			}
		}
	}
	for column, types := range schemas {
		m.set(MetaFileSchema, column, strings.Join(slices.Sorted(maps.Keys(types)), " | "))
	}
	for column, columnCodecs := range codecs {
		m.set(MetaProperties, "codec."+column, strings.Join(slices.Sorted(maps.Keys(columnCodecs)), " | "))
	}
	return m, nil
}

// parquetColumnType is a leaf column's physical type with its width, logical type and repetition:
// the parts a reader decodes by, which Spark's schema folds together (INT96 and INT64 timestamps both
// read as timestamp).
func parquetColumnType(node parquet.Node) string {
	logical := "-"
	if lt := node.Type().LogicalType(); lt != nil {
		logical = lt.String()
	}
	repetition := "required"
	switch {
	case node.Repeated():
		repetition = "repeated"
	case node.Optional():
		repetition = "optional"
	}
	return fmt.Sprintf("%s %s %s", parquetKind(node), logical, repetition)
}
