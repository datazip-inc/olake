package testutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/parquet-go/parquet-go"
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
		reader, err := client.GetObject(ctx, ParquetTestBucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch %s: %s", object.Key, err)
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %s", object.Key, err)
		}
		file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %s", object.Key, err)
		}
		columns := map[string]string{}
		for _, path := range file.Schema().Columns() {
			leaf, _ := file.Schema().Lookup(path...)
			kind := leaf.Node.Type().Kind().String()
			if leaf.Node.Type().Kind() == parquet.FixedLenByteArray {
				kind = fmt.Sprintf("%s(%d)", kind, leaf.Node.Type().Length())
			}
			columns[strings.Join(path, ".")] = kind
		}
		kinds[object.Key] = columns
	}
	return kinds, nil
}

// parquetTablePath is the MinIO key prefix a stream's parquet files are written under.
func parquetTablePath(parquetDB, tableName string) string {
	return fmt.Sprintf("%s/%s/", parquetDB, tableName)
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
