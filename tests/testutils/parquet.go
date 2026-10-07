package testutils

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

// parquetTablePath is the MinIO key prefix a stream's parquet files are written under.
func parquetTablePath(parquetDB, tableName string) string {
	return fmt.Sprintf("%s/%s/", parquetDB, tableName)
}

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
