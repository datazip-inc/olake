package destinationtest

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// parquetKind writes Parquet files under a folder per test.
type parquetKind struct {
	name     string
	services []*testhandler.Service
	config   map[string]any
	// drop removes one test's folder.
	drop func(ctx context.Context, database string) error
}

func (k *parquetKind) Name() string {
	return k.name
}

func (k *parquetKind) Services() []*testhandler.Service {
	return k.services
}

// Open refuses a second Parquet kind in one test binary. The Parquet writer keeps its first config
// for the life of the process, so the second kind would write to the first.
func (k *parquetKind) Open(_ context.Context, t testing.TB) (*types.WriterConfig, error) {
	parquetPin.Lock()
	defer parquetPin.Unlock()
	if parquetPin.kind == nil {
		parquetPin.kind, parquetPin.test = k, t.Name()
	}
	if parquetPin.kind != k {
		return nil, fmt.Errorf("%s already uses %s in this test binary, and the Parquet writer keeps its first config for the whole process; use %s from another package",
			parquetPin.test, parquetPin.kind.name, k.name)
	}
	return &types.WriterConfig{Type: types.Parquet, WriterConfig: maps.Clone(k.config)}, nil
}

func (k *parquetKind) Drop(ctx context.Context, database string) error {
	return k.drop(ctx, database)
}

// parquetPin remembers the first Parquet kind this process handed out.
var parquetPin struct {
	sync.Mutex
	kind *parquetKind
	test string
}

// localRoot is the folder ParquetLocal writes under.
var localRoot = filepath.Join(os.TempDir(), "olake-testhandler", "parquet")

// ParquetLocal writes Parquet files under a local folder.
var ParquetLocal testhandler.DestinationKind = &parquetKind{
	name:   "parquet-local",
	config: map[string]any{"local_path": localRoot},
	drop: func(_ context.Context, database string) error {
		return os.RemoveAll(filepath.Join(localRoot, database))
	},
}

// ParquetMinIO writes Parquet files to an S3 bucket served by a MinIO container.
var ParquetMinIO testhandler.DestinationKind = &parquetKind{
	name:     "parquet-minio",
	services: []*testhandler.Service{minioService},
	config: map[string]any{
		"s3_bucket":     bucket,
		"s3_region":     region,
		"s3_access_key": serverUser,
		"s3_secret_key": serverKey,
		"s3_endpoint":   "http://" + minioService.Addr(),
		"s3_path":       "parquet",
	},
	drop: func(ctx context.Context, database string) error {
		return removePrefix(ctx, "parquet/"+database)
	},
}
