package parquet

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestLoad2PCState(t *testing.T) {
	ctx := context.Background()

	// With no staging work, setup returns the checkpoint from metadata.json.
	t.Run("returns durable metadata", func(t *testing.T) {
		p, store := testS3Parquet(t, "current-thread", false)
		expected := &types.MetadataState{ID: "incremental-thread", State: `{"cursor":10}`}
		testWriteMetadata(t, p, store, expected)

		state, err := p.load2PCState(ctx)
		require.NoError(t, err)
		require.Equal(t, expected, state)
	})

	// A finished full-refresh chunk is promoted and added to committed chunk IDs.
	t.Run("recovers finished full refresh staging", func(t *testing.T) {
		p, store := testS3Parquet(t, "current-thread", true)
		threadID := "full-refresh-thread"
		prefix := p.stagingRootPrefix() + encodeThreadID(threadID) + "/"
		store.put(prefix+"bucket_1/data.parquet", []byte("full-refresh"))
		store.put(prefix+parquet2PCFinishFile, []byte("{}"))

		state, err := p.load2PCState(ctx)
		require.NoError(t, err)
		require.Contains(t, state.FullRefreshCommittedIDs, threadID)
		require.True(t, *state.DedupInserts)
		require.Equal(t, []byte("full-refresh"), store.get(p.s3ObjectPath(p.basePath+"/bucket_1/data.parquet")))
		require.Empty(t, store.keys(prefix))
	})

	// A finished shared commit restores its data and CDC/incremental checkpoint.
	t.Run("recovers finished shared staging", func(t *testing.T) {
		p, store := testS3Parquet(t, "cdc-thread", false)
		expected := &types.MetadataState{State: `{"lsn":"1/20"}`}
		finishData, _, err := finishState(expected)
		require.NoError(t, err)
		store.put(p.stagingObjectKey("bucket_1/data.parquet"), []byte("cdc"))
		store.put(p.currentFinishObjectKey(), finishData)

		state, err := p.load2PCState(ctx)
		require.NoError(t, err)
		require.Equal(t, expected, state)
		require.Equal(t, []byte("cdc"), store.get(p.s3ObjectPath(p.basePath+"/bucket_1/data.parquet")))
		require.Empty(t, store.keys(p.stagingRootPrefix()))
	})

	// Full-refresh staging without finish.json is not committed and is discarded.
	t.Run("deletes unfinished full refresh staging", func(t *testing.T) {
		p, store := testS3Parquet(t, "current-thread", true)
		prefix := p.stagingRootPrefix() + encodeThreadID("failed-thread") + "/"
		store.put(prefix+"orphan.parquet", []byte("incomplete"))

		state, err := p.load2PCState(ctx)
		require.NoError(t, err)
		require.Nil(t, state)
		require.Empty(t, store.keys(prefix))
	})

	// Shared staging without finish.json is not committed and is discarded.
	t.Run("deletes unfinished shared staging", func(t *testing.T) {
		p, store := testS3Parquet(t, "cdc-thread", false)
		store.put(p.stagingObjectKey("orphan.parquet"), []byte("incomplete"))

		state, err := p.load2PCState(ctx)
		require.NoError(t, err)
		require.Nil(t, state)
		require.Empty(t, store.keys(p.stagingRootPrefix()))
	})

	// Recovery promotes only objects that remain after an interrupted copy.
	t.Run("continues partial promotion", func(t *testing.T) {
		p, store := testS3Parquet(t, "incremental-thread", false)
		expected := &types.MetadataState{ID: "incremental-thread", State: `{"cursor":30}`}
		finishData, _, err := finishState(expected)
		require.NoError(t, err)
		store.put(p.s3ObjectPath(p.basePath+"/bucket_1/already-promoted.parquet"), []byte("first"))
		store.put(p.stagingObjectKey("bucket_2/remaining.parquet"), []byte("second"))
		store.put(p.currentFinishObjectKey(), finishData)

		state, err := p.load2PCState(ctx)
		require.NoError(t, err)
		require.Equal(t, expected, state)
		require.Equal(t, []byte("first"), store.get(p.s3ObjectPath(p.basePath+"/bucket_1/already-promoted.parquet")))
		require.Equal(t, []byte("second"), store.get(p.s3ObjectPath(p.basePath+"/bucket_2/remaining.parquet")))
		require.Empty(t, store.keys(p.stagingRootPrefix()))
	})

	// Malformed finish metadata stops recovery instead of discarding staged data.
	t.Run("rejects invalid finish state", func(t *testing.T) {
		p, store := testS3Parquet(t, "invalid-thread", false)
		store.put(p.currentFinishObjectKey(), []byte("{"))

		_, err := p.load2PCState(ctx)
		require.ErrorContains(t, err, "failed to unmarshal parquet 2pc finish state")
	})

	// Full-refresh finish files are markers and must not contain cursor metadata.
	t.Run("rejects metadata in full refresh finish", func(t *testing.T) {
		p, store := testS3Parquet(t, "current-thread", true)
		prefix := p.stagingRootPrefix() + encodeThreadID("invalid-thread") + "/"
		store.put(prefix+parquet2PCFinishFile, []byte(`{"state":"cursor"}`))

		_, err := p.load2PCState(ctx)
		require.ErrorContains(t, err, "metadata state is not supported in full-refresh staging")
	})
}

func TestStagingPaths(t *testing.T) {
	tests := []struct {
		name           string
		backfill       bool
		expectedPrefix string
	}{
		{
			name:           "full refresh",
			backfill:       true,
			expectedPrefix: "root/namespace/table/_olake_2pc/dGhyZWFk/",
		},
		{
			name:           "cdc and incremental",
			expectedPrefix: "root/namespace/table/_olake_2pc/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := testS3Parquet(t, "thread", tt.backfill)
			p.config.Prefix = "root"

			require.Equal(t, tt.expectedPrefix, p.currentStagingPrefix())
			require.Equal(t, tt.expectedPrefix+"partition/data.parquet", p.stagingObjectKey("partition/data.parquet"))
			require.Equal(t, tt.expectedPrefix+parquet2PCFinishFile, p.currentFinishObjectKey())
			require.Equal(t, "root/namespace/table/metadata.json", p.metadataObjectKey())
		})
	}
}

func TestCloseCommits2PCState(t *testing.T) {
	ctx := context.Background()
	dedupInserts := true
	tests := []struct {
		name          string
		threadID      string
		backfill      bool
		writeRecord   bool
		existingState *types.MetadataState
		metadataState *types.MetadataState
		expectedState *types.MetadataState
		expectedFiles int
		expectedError string
	}{
		// Full refresh commits the chunk ID after promoting its data.
		{
			name:        "full refresh",
			threadID:    "full-refresh-thread",
			backfill:    true,
			writeRecord: true,
			expectedState: &types.MetadataState{
				FullRefreshCommittedIDs: []string{"full-refresh-thread"},
				DedupInserts:            &dedupInserts,
			},
			expectedFiles: 1,
		},
		// Incremental state is updated without dropping full-refresh progress.
		{
			name:          "incremental",
			threadID:      "incremental-thread",
			writeRecord:   true,
			existingState: &types.MetadataState{FullRefreshCommittedIDs: []string{"full-refresh-thread"}},
			metadataState: &types.MetadataState{ID: "incremental-thread", State: `{"cursor":30}`},
			expectedState: &types.MetadataState{
				ID:                      "incremental-thread",
				State:                   `{"cursor":30}`,
				FullRefreshCommittedIDs: []string{"full-refresh-thread"},
			},
			expectedFiles: 1,
		},
		// Empty full-refresh chunks still need a durable committed chunk ID.
		{
			name:     "empty full refresh",
			threadID: "empty-full-refresh-thread",
			backfill: true,
			expectedState: &types.MetadataState{
				FullRefreshCommittedIDs: []string{"empty-full-refresh-thread"},
				DedupInserts:            &dedupInserts,
			},
		},
		// No-op CDC/incremental closes keep the last data-backed checkpoint.
		{
			name:          "no data",
			threadID:      "cdc-thread",
			existingState: &types.MetadataState{State: `{"lsn":"1/1"}`},
			metadataState: &types.MetadataState{State: `{"lsn":"1/2"}`},
			expectedState: &types.MetadataState{State: `{"lsn":"1/1"}`},
		},
		// A fresh no-op CDC/incremental close has no destination progress to persist.
		{
			name:          "no data without durable metadata",
			threadID:      "new-cdc-thread",
			metadataState: &types.MetadataState{State: `{"lsn":"1/1"}`},
		},
		// Data cannot be committed without the matching recovery checkpoint.
		{
			name:          "data without metadata",
			threadID:      "missing-metadata-thread",
			writeRecord:   true,
			expectedError: "cannot commit parquet CDC or incremental files without metadata state",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, store := testS3Parquet(t, tt.threadID, tt.backfill)
			if tt.existingState != nil {
				testWriteMetadata(t, p, store, tt.existingState)
			}
			if tt.writeRecord {
				require.NoError(t, p.Write(ctx, []types.RawRecord{testRawRecord()}))
			}

			var metadataState any
			if tt.metadataState != nil {
				metadataState = tt.metadataState
			}
			err := p.Close(ctx, metadataState)
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			state, readErr := p.readMetadata(ctx)
			require.NoError(t, readErr)
			require.Equal(t, tt.expectedState, state)
			require.Equal(t, tt.expectedFiles, testFinalParquetObjects(p, store))
			require.Empty(t, store.keys(p.stagingRootPrefix()))
			if p.tempDir != "" {
				require.NoDirExists(t, p.tempDir)
			}
		})
	}
}

func TestCloseCommitsRolledFiles(t *testing.T) {
	ctx := context.Background()
	p, store := testS3Parquet(t, "incremental-thread", false)
	p.maxFileBytes = 1
	p.checkIntervalForRoll = 1

	records := []types.RawRecord{testRawRecord(), testRawRecord(), testRawRecord()}
	require.NoError(t, p.Write(ctx, records))
	require.Len(t, p.pendingDataFiles(), len(records))

	state := &types.MetadataState{ID: "incremental-thread", State: `{"cursor":30}`}
	require.NoError(t, p.Close(ctx, state))

	require.Equal(t, len(records), testFinalParquetObjects(p, store))
	require.Empty(t, store.keys(p.stagingRootPrefix()))
	metadata, err := p.readMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, state, metadata)
}

func TestCloseRecoversSharedStagingBeforeUpload(t *testing.T) {
	ctx := context.Background()
	p, store := testS3Parquet(t, "next-cdc-thread", false)
	store.put(p.stagingObjectKey("orphan.parquet"), []byte("incomplete"))
	require.NoError(t, p.Write(ctx, []types.RawRecord{testRawRecord()}))

	state := &types.MetadataState{State: `{"lsn":"1/30"}`}
	require.NoError(t, p.Close(ctx, state))
	require.Nil(t, store.get(p.s3ObjectPath(p.basePath+"/orphan.parquet")))
	require.Equal(t, 1, testFinalParquetObjects(p, store))
	require.Empty(t, store.keys(p.stagingRootPrefix()))
}

func TestCloseRecoveryBoundaries(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name            string
		fail            func(p *Parquet, store *memoryS3)
		expectCommitted bool
	}{
		// Upload failures occur before finish.json and remain uncommitted.
		{
			name: "during upload",
			fail: func(p *Parquet, store *memoryS3) {
				store.failNext(memoryS3Put, p.stagingObjectKey(p.pendingDataFiles()[0].relativePath))
			},
		},
		// finish.json is the boundary between discard and roll-forward recovery.
		{
			name: "before finish",
			fail: func(p *Parquet, store *memoryS3) {
				store.failNext(memoryS3Put, p.currentFinishObjectKey())
			},
		},
		// Promotion can resume from the remaining staged objects.
		{
			name: "during promotion",
			fail: func(p *Parquet, store *memoryS3) {
				store.failNext(memoryS3Copy, p.stagingObjectKey(p.pendingDataFiles()[0].relativePath))
			},
			expectCommitted: true,
		},
		// A completed promotion can recreate metadata from finish.json.
		{
			name: "during metadata commit",
			fail: func(p *Parquet, store *memoryS3) {
				store.failNext(memoryS3Put, p.metadataObjectKey())
			},
			expectCommitted: true,
		},
		// Cleanup failure leaves finish.json available for an idempotent replay.
		{
			name: "during staging cleanup",
			fail: func(p *Parquet, store *memoryS3) {
				store.failNext(memoryS3Delete, p.currentFinishObjectKey())
			},
			expectCommitted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, store := testS3Parquet(t, "incremental-thread", false)
			require.NoError(t, p.Write(ctx, []types.RawRecord{testRawRecord()}))
			tt.fail(p, store)

			expected := &types.MetadataState{ID: "incremental-thread", State: `{"cursor":30}`}
			require.Error(t, p.Close(ctx, expected))

			recovery := testS3ParquetWithStore(t, store, "next-thread", false)
			state, err := recovery.load2PCState(ctx)
			require.NoError(t, err)
			require.Empty(t, store.keys(recovery.stagingRootPrefix()))
			if tt.expectCommitted {
				require.Equal(t, expected, state)
				require.Equal(t, 1, testFinalParquetObjects(recovery, store))
			} else {
				require.Nil(t, state)
				require.Zero(t, testFinalParquetObjects(recovery, store))
			}
		})
	}
}

// TestCloseCancellationDoesNotStageFiles verifies canceled writers remain private to local temp storage.
func TestCloseCancellationDoesNotStageFiles(t *testing.T) {
	p, store := testS3Parquet(t, "canceled-thread", false)
	p.maxFileBytes = 1
	p.checkIntervalForRoll = 1
	require.NoError(t, p.Write(context.Background(), []types.RawRecord{testRawRecord(), testRawRecord(), testRawRecord()}))
	require.Len(t, p.pendingDataFiles(), 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, p.Close(ctx, &types.MetadataState{State: `{"lsn":"1/20"}`}), context.Canceled)

	require.Empty(t, store.keys(p.stagingRootPrefix()))
	require.Zero(t, testFinalParquetObjects(p, store))
	require.NoDirExists(t, p.tempDir)
}

func TestCommitMetadataMergesParallelKafkaCheckpoints(t *testing.T) {
	ctx := context.Background()
	first, store := testS3Parquet(t, "reader-1", false)
	second := testS3ParquetWithStore(t, store, "reader-2", false)

	require.NoError(t, first.Write(ctx, []types.RawRecord{testRawRecord()}))
	require.NoError(t, first.Close(ctx, &types.MetadataState{
		State: `{"consumer_group_id":"group","partition_0":10}`,
	}))
	require.NoError(t, second.Write(ctx, []types.RawRecord{testRawRecord()}))
	require.NoError(t, second.Close(ctx, &types.MetadataState{
		State: `{"consumer_group_id":"group","partition_1":20}`,
	}))

	state, err := second.readMetadata(ctx)
	require.NoError(t, err)
	var checkpoint map[string]any
	decoder := json.NewDecoder(strings.NewReader(state.State.(string)))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&checkpoint))
	require.Equal(t, "group", checkpoint["consumer_group_id"])
	require.Equal(t, json.Number("10"), checkpoint["partition_0"])
	require.Equal(t, json.Number("20"), checkpoint["partition_1"])
	require.Equal(t, 2, testFinalParquetObjects(second, store))
	require.Empty(t, store.keys(second.stagingRootPrefix()))
}

func TestLocalCloseKeepsExistingBehavior(t *testing.T) {
	ctx := context.Background()
	localPath := t.TempDir()
	p := &Parquet{config: &Config{Path: localPath}}
	_, state, err := p.Setup(ctx, testConfiguredStream(), nil, &destination.Options{ThreadID: "local-thread"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NoError(t, p.Write(ctx, []types.RawRecord{testRawRecord()}))
	require.NoError(t, p.Close(ctx, nil))

	require.Equal(t, 1, testLocalParquetFiles(t, filepath.Join(localPath, p.basePath)))
	require.NoFileExists(t, filepath.Join(localPath, p.basePath, parquet2PCMetadataFile))
	require.NoDirExists(t, filepath.Join(localPath, p.basePath, parquet2PCDir))
}

type memoryS3 struct {
	s3iface.S3API
	mu       sync.Mutex
	objects  map[string][]byte
	failures map[string]error
}

func newMemoryS3() *memoryS3 {
	return &memoryS3{
		objects:  make(map[string][]byte),
		failures: make(map[string]error),
	}
}

func (m *memoryS3) PutObjectWithContext(_ aws.Context, input *s3.PutObjectInput, _ ...request.Option) (*s3.PutObjectOutput, error) {
	key := aws.StringValue(input.Key)
	if err := m.failure(memoryS3Put, key); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.put(key, data)
	return &s3.PutObjectOutput{}, nil
}

func (m *memoryS3) GetObjectWithContext(_ aws.Context, input *s3.GetObjectInput, _ ...request.Option) (*s3.GetObjectOutput, error) {
	data := m.get(aws.StringValue(input.Key))
	if data == nil {
		return nil, awserr.New(s3.ErrCodeNoSuchKey, "object not found", nil)
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(string(data)))}, nil
}

func (m *memoryS3) ListObjectsPagesWithContext(_ aws.Context, input *s3.ListObjectsInput, fn func(*s3.ListObjectsOutput, bool) bool, _ ...request.Option) error {
	keys := m.keys(aws.StringValue(input.Prefix))
	objects := make([]*s3.Object, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, &s3.Object{Key: aws.String(key)})
	}
	fn(&s3.ListObjectsOutput{Contents: objects}, true)
	return nil
}

func (m *memoryS3) CopyObjectWithContext(_ aws.Context, input *s3.CopyObjectInput, _ ...request.Option) (*s3.CopyObjectOutput, error) {
	source := strings.TrimPrefix(aws.StringValue(input.CopySource), aws.StringValue(input.Bucket)+"/")
	source, err := url.PathUnescape(source)
	if err != nil {
		return nil, err
	}
	if err := m.failure(memoryS3Copy, source); err != nil {
		return nil, err
	}
	data := m.get(source)
	if data == nil {
		return nil, awserr.New(s3.ErrCodeNoSuchKey, "copy source not found", nil)
	}
	m.put(aws.StringValue(input.Key), data)
	return &s3.CopyObjectOutput{}, nil
}

func (m *memoryS3) DeleteObjectWithContext(_ aws.Context, input *s3.DeleteObjectInput, _ ...request.Option) (*s3.DeleteObjectOutput, error) {
	key := aws.StringValue(input.Key)
	if err := m.failure(memoryS3Delete, key); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

const (
	memoryS3Put    = "put"
	memoryS3Copy   = "copy"
	memoryS3Delete = "delete"
)

func (m *memoryS3) failNext(operation, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[operation+":"+key] = awserr.New("TestFailure", "injected s3 failure", nil)
}

func (m *memoryS3) failure(operation, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	failureKey := operation + ":" + key
	err := m.failures[failureKey]
	delete(m.failures, failureKey)
	return err
}

func (m *memoryS3) put(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), data...)
}

func (m *memoryS3) get(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, exists := m.objects[key]
	if !exists {
		return nil
	}
	return append([]byte(nil), data...)
}

func (m *memoryS3) keys(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

type memoryUploader struct {
	store *memoryS3
}

func (u *memoryUploader) Upload(input *s3manager.UploadInput, _ ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	return u.UploadWithContext(context.Background(), input)
}

func (u *memoryUploader) UploadWithContext(_ aws.Context, input *s3manager.UploadInput, _ ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	key := aws.StringValue(input.Key)
	if err := u.store.failure(memoryS3Put, key); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	u.store.put(key, data)
	return &s3manager.UploadOutput{}, nil
}

func testS3Parquet(t *testing.T, threadID string, backfill bool) (*Parquet, *memoryS3) {
	t.Helper()
	store := newMemoryS3()
	return testS3ParquetWithStore(t, store, threadID, backfill), store
}

func testS3ParquetWithStore(t *testing.T, store *memoryS3, threadID string, backfill bool) *Parquet {
	t.Helper()
	return &Parquet{
		config:           &Config{Path: filepath.Join(t.TempDir(), "cache"), Bucket: "bucket"},
		options:          &destination.Options{ThreadID: threadID, Backfill: backfill},
		stream:           testConfiguredStream(),
		basePath:         filepath.Join("namespace", "table"),
		partitionedFiles: make(map[string][]*FileMetadata),
		s3Client:         store,
		s3Uploader:       &memoryUploader{store: store},
	}
}

func testWriteMetadata(t *testing.T, p *Parquet, store *memoryS3, state *types.MetadataState) {
	t.Helper()
	data, err := json.Marshal(state)
	require.NoError(t, err)
	store.put(p.metadataObjectKey(), data)
}

func testFinalParquetObjects(p *Parquet, store *memoryS3) int {
	prefix := p.s3ObjectPath(p.basePath) + "/"
	var count int
	for _, key := range store.keys(prefix) {
		if strings.Contains(key, "/"+parquet2PCDir+"/") {
			continue
		}
		if filepath.Ext(key) == "."+constants.ParquetFileExt {
			count++
		}
	}
	return count
}

func testLocalParquetFiles(t *testing.T, root string) int {
	t.Helper()
	var count int
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		if !entry.IsDir() && filepath.Ext(path) == "."+constants.ParquetFileExt {
			count++
		}
		return nil
	}))
	return count
}

func testRawRecord() types.RawRecord {
	return types.CreateRawRecord(
		map[string]any{"id": 1},
		map[string]any{
			constants.OlakeID:        "row-1",
			constants.OlakeTimestamp: time.Now().UTC(),
			constants.OpType:         "r",
		},
	)
}

func testConfiguredStream() *types.ConfiguredStream {
	stream := types.NewStream("users", "public", nil)
	stream.UpsertField("id", types.Int64, false, false)
	return &types.ConfiguredStream{
		Stream: stream,
		StreamMetadata: types.StreamMetadata{
			Normalization: false,
		},
	}
}
