package parquet

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
)

func testMemoryS3Store() (*s3Store, *memoryS3) {
	inner := newMemoryS3()
	return &s3Store{
		client:   inner,
		uploader: &memoryUploader{store: inner},
		bucket:   "bucket",
	}, inner
}

func TestS3ObjectKey(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		relative string
		want     string
	}{
		{
			name:     "no prefix",
			relative: "namespace/table/data.parquet",
			want:     "namespace/table/data.parquet",
		},
		{
			name:     "joins s3 path",
			prefix:   "root",
			relative: "namespace/table/data.parquet",
			want:     "root/namespace/table/data.parquet",
		},
		{
			name:     "nested s3 path",
			prefix:   "allowed/path",
			relative: "namespace/table/_olake_2pc/finish.json",
			want:     "allowed/path/namespace/table/_olake_2pc/finish.json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &s3Store{prefix: tt.prefix}
			require.Equal(t, tt.want, store.ObjectKey(tt.relative))
		})
	}
}

func TestS3IsNotFound(t *testing.T) {
	store := &s3Store{}
	require.False(t, store.IsNotFound(nil))
	require.False(t, store.IsNotFound(errors.New("other")))
	require.True(t, store.IsNotFound(awserr.New(s3.ErrCodeNoSuchKey, "missing", nil)))
	require.True(t, store.IsNotFound(awserr.New("NotFound", "missing", nil)))
	require.False(t, store.IsNotFound(awserr.New("AccessDenied", "denied", nil)))
}

func TestNewS3Store(t *testing.T) {
	t.Run("trims s3 path", func(t *testing.T) {
		store, err := newS3Store(&Config{Bucket: "bucket", Region: "us-east-1", Prefix: "/root/"})
		require.NoError(t, err)
		require.Equal(t, "s3", store.Kind())
		require.Equal(t, "bucket", store.bucket)
		require.Equal(t, "root", store.prefix)
		require.Equal(t, "root/namespace/table", store.ObjectKey("namespace/table"))
		require.False(t, store.gcs)
	})

	t.Run("custom endpoint uses path style", func(t *testing.T) {
		store, err := newS3Store(&Config{
			Bucket:     "bucket",
			Region:     "us-east-1",
			S3Endpoint: "https://storage.googleapis.com",
		})
		require.NoError(t, err)
		client, ok := store.client.(*s3.S3)
		require.True(t, ok)
		require.Equal(t, "https://storage.googleapis.com", aws.StringValue(client.Config.Endpoint))
		require.True(t, aws.BoolValue(client.Config.S3ForcePathStyle))
		require.True(t, store.gcs)
	})
}

func TestS3DeletePrefixGCSUsesIndividualDeletes(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()
	store.gcs = true
	inner.put("root/namespace/table/data.parquet", []byte("table-data"))
	inner.put("root/namespace/table/_olake_2pc/staged.parquet", []byte("staged"))
	inner.put("root/namespace/table_backup/data.parquet", []byte("sibling"))
	require.NoError(t, store.DeletePrefix(ctx, "root/namespace/table/"))
	require.Empty(t, inner.keys("root/namespace/table/"))
	require.Equal(t, []byte("sibling"), inner.get("root/namespace/table_backup/data.parquet"))
}

func TestS3PutGetListCopyDelete(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()

	require.NoError(t, store.Put(ctx, "root/namespace/table/data.parquet", []byte("table-data")))
	got, err := store.Get(ctx, "root/namespace/table/data.parquet")
	require.NoError(t, err)
	require.Equal(t, []byte("table-data"), got)

	_, err = store.Get(ctx, "root/namespace/table/missing.parquet")
	require.Error(t, err)
	require.True(t, store.IsNotFound(err))

	require.NoError(t, store.Copy(ctx, "root/namespace/table/data.parquet", "root/namespace/table/copy.parquet"))
	require.Equal(t, []byte("table-data"), inner.get("root/namespace/table/copy.parquet"))

	keys, err := store.List(ctx, "root/namespace/table/")
	require.NoError(t, err)
	require.Equal(t, []string{"root/namespace/table/copy.parquet", "root/namespace/table/data.parquet"}, keys)

	require.NoError(t, store.Delete(ctx, "root/namespace/table/copy.parquet"))
	require.Nil(t, inner.get("root/namespace/table/copy.parquet"))
}

func TestS3UploadFile(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()

	file, err := os.CreateTemp(t.TempDir(), "upload-*.parquet")
	require.NoError(t, err)
	_, err = file.Write([]byte("file-bytes"))
	require.NoError(t, err)
	_, err = file.Seek(0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	require.NoError(t, store.UploadFile(ctx, "root/namespace/table/upload.parquet", file))
	require.Equal(t, []byte("file-bytes"), inner.get("root/namespace/table/upload.parquet"))
}

func TestS3DeletePrefix(t *testing.T) {
	ctx := context.Background()
	store, inner := testMemoryS3Store()
	inner.put("root/namespace/table/data.parquet", []byte("table-data"))
	inner.put("root/namespace/table/_olake_2pc/staged.parquet", []byte("staged"))
	inner.put("root/namespace/table_backup/data.parquet", []byte("sibling"))

	require.NoError(t, store.DeletePrefix(ctx, "root/namespace/table/"))
	require.Empty(t, inner.keys("root/namespace/table/"))
	require.Equal(t, []byte("sibling"), inner.get("root/namespace/table_backup/data.parquet"))
}
