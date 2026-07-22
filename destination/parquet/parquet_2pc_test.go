package parquet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestLoad2PCState(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// metadata.json is the durable recovery state when no staging recovery is pending.
		{
			name: "returns durable metadata",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "current-thread")
				expected := &types.MetadataState{ID: "incremental-thread", State: `{"cursor":10}`}
				testWriteMetadata(t, p, expected)

				state, err := p.load2PCState(ctx, true)
				require.NoError(t, err)
				require.Equal(t, expected, state)
			},
		},
		// A finished full-refresh chunk is promoted and appended to durable metadata.
		{
			name: "recovers finished full refresh staging",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "full-refresh-thread")
				stagedFile := testWriteStagedFile(t, p, "bucket_1/data.parquet")
				testWriteFinish(t, p, nil)

				state, err := p.load2PCState(ctx, true)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "full-refresh-thread"))
				require.NotNil(t, state.DedupInserts)
				require.True(t, *state.DedupInserts)
				requirePathExists(t, filepath.Join(p.config.Path, p.basePath, "bucket_1", "data.parquet"))
				requirePathNotExists(t, stagedFile)
				requirePathNotExists(t, p.localStagingPath("full-refresh-thread"))
			},
		},
		// A finished incremental attempt restores its cursor together with promoted data.
		{
			name: "recovers finished metadata staging",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "incremental-thread")
				testWriteStagedFile(t, p, "bucket_1/data.parquet")
				expected := &types.MetadataState{ID: "incremental-thread", State: `{"cursor":20}`}
				testWriteFinish(t, p, expected)

				state, err := p.load2PCState(ctx, true)
				require.NoError(t, err)
				require.Equal(t, expected, state)
				requirePathExists(t, filepath.Join(p.config.Path, p.basePath, "bucket_1", "data.parquet"))
				requirePathNotExists(t, p.localStagingPath("incremental-thread"))
			},
		},
		// The first setup removes data from an attempt that never reached finish.json.
		{
			name: "deletes incomplete staging on first setup",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "failed-thread")
				stagedFile := testWriteStagedFile(t, p, "failed.parquet")

				state, err := p.load2PCState(ctx, true)
				require.NoError(t, err)
				require.Nil(t, state)
				requirePathNotExists(t, stagedFile)
				requirePathNotExists(t, p.localStagingPath("failed-thread"))
			},
		},
		// Later setups leave unfinished staging alone because another writer may still own it.
		{
			name: "preserves active staging after first setup",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "active-thread")
				stagedFile := testWriteStagedFile(t, p, "active.parquet")

				state, err := p.load2PCState(ctx, false)
				require.NoError(t, err)
				require.Nil(t, state)
				requirePathExists(t, stagedFile)
			},
		},
		// finish.json remains after promotion, so setup can commit metadata after an interruption.
		{
			name: "recovers after promotion before metadata commit",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "promoted-thread")
				testWriteStagedFile(t, p, "bucket_1/data.parquet")
				testWriteFinish(t, p, nil)
				require.NoError(t, p.promoteStaging(ctx, "promoted-thread"))

				state, err := p.load2PCState(ctx, true)
				require.NoError(t, err)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "promoted-thread"))
				requirePathExists(t, filepath.Join(p.config.Path, p.basePath, "bucket_1", "data.parquet"))
				requirePathNotExists(t, p.localStagingPath("promoted-thread"))
			},
		},
		// Replaying a finish marker after metadata commit does not duplicate a chunk ID.
		{
			name: "replays committed finish idempotently",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "committed-thread")
				testWriteFinish(t, p, nil)
				_, err := p.commitMetadata(ctx, "committed-thread", nil)
				require.NoError(t, err)

				state, err := p.load2PCState(ctx, true)
				require.NoError(t, err)
				require.Equal(t, []string{"committed-thread"}, state.FullRefreshCommittedIDs)
				requirePathNotExists(t, p.localStagingPath("committed-thread"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestCloseCommits2PCState(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		threadID      string
		backfill      bool
		writeRecord   bool
		existingState *types.MetadataState
		metadataState *types.MetadataState
		expectedError string
		verify        func(t *testing.T, p *Parquet, state *types.MetadataState)
	}{
		// Full refresh commits the chunk ID after its data is promoted.
		{
			name:        "full refresh",
			threadID:    "full-refresh-thread",
			backfill:    true,
			writeRecord: true,
			verify: func(t *testing.T, p *Parquet, state *types.MetadataState) {
				require.Equal(t, []string{"full-refresh-thread"}, state.FullRefreshCommittedIDs)
				require.Equal(t, 1, testFinalParquetFiles(t, p))
			},
		},
		// Incremental commits the cursor state without dropping full-refresh progress.
		{
			name:          "incremental",
			threadID:      "incremental-thread",
			writeRecord:   true,
			metadataState: &types.MetadataState{ID: "incremental-thread", State: `{"cursor":30}`},
			verify: func(t *testing.T, p *Parquet, state *types.MetadataState) {
				require.Equal(t, "incremental-thread", state.ID)
				require.Equal(t, `{"cursor":30}`, state.State)
				require.Equal(t, 1, testFinalParquetFiles(t, p))
			},
		},
		// A full-refresh chunk is committed even when it produces no data file.
		{
			name:     "empty full refresh",
			threadID: "empty-full-refresh-thread",
			backfill: true,
			verify: func(t *testing.T, p *Parquet, state *types.MetadataState) {
				require.Equal(t, []string{"empty-full-refresh-thread"}, state.FullRefreshCommittedIDs)
				require.Equal(t, 0, testFinalParquetFiles(t, p))
			},
		},
		// A no-op CDC/incremental sync does not replace the last data-backed commit.
		{
			name:          "no data",
			threadID:      "cdc-thread",
			existingState: &types.MetadataState{State: `{"lsn":"1/1"}`},
			metadataState: &types.MetadataState{State: `{"lsn":"1/2"}`},
			verify: func(t *testing.T, p *Parquet, state *types.MetadataState) {
				require.Equal(t, `{"lsn":"1/1"}`, state.State)
				require.Equal(t, 0, testFinalParquetFiles(t, p))
			},
		},
		// A CDC/incremental writer without data or metadata has no destination progress to commit.
		{
			name:     "no progress",
			threadID: "no-progress-thread",
		},
		// Data cannot become visible unless its CDC/incremental recovery state is committed with it.
		{
			name:          "data without metadata",
			threadID:      "missing-metadata-thread",
			writeRecord:   true,
			expectedError: "cannot commit parquet CDC or incremental files without metadata state",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Parquet{config: &Config{Path: t.TempDir()}}
			_, state, err := p.Setup(ctx, testConfiguredStream(), nil, &destination.Options{ThreadID: tt.threadID, Backfill: tt.backfill})
			require.NoError(t, err)
			require.Nil(t, state)
			if tt.existingState != nil {
				testWriteMetadata(t, p, tt.existingState)
			}

			if tt.writeRecord {
				require.NoError(t, p.Write(ctx, []types.RawRecord{testRawRecord()}))
			}
			var metadataState any
			if tt.metadataState != nil {
				metadataState = tt.metadataState
			}
			err = p.Close(ctx, metadataState)
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			state, err = p.readMetadata(ctx)
			require.NoError(t, err)
			if tt.verify == nil {
				require.Nil(t, state)
				require.Equal(t, 0, testFinalParquetFiles(t, p))
			} else {
				require.NotNil(t, state)
				tt.verify(t, p, state)
			}
			requirePathNotExists(t, p.localStagingPath(tt.threadID))
			if state != nil {
				requirePathExists(t, filepath.Join(p.config.Path, p.metadataPath()))
			}
		})
	}
}

func TestLoad2PCStateKeepsWriterStagingIsolated(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	first := testParquet2PCWithPath(baseDir, "first-thread")
	second := testParquet2PCWithPath(baseDir, "second-thread")

	testWriteStagedFile(t, first, "first.parquet")
	secondFile := testWriteStagedFile(t, second, "second.parquet")
	testWriteFinish(t, first, &types.MetadataState{State: `{"lsn":"1/1"}`})

	state, err := first.load2PCState(ctx, false)
	require.NoError(t, err)
	require.Equal(t, `{"lsn":"1/1"}`, state.State)
	requirePathExists(t, secondFile)
	requirePathNotExists(t, first.localStagingPath("first-thread"))
}

func TestCommitMetadataPreservesFullRefreshState(t *testing.T) {
	ctx := context.Background()
	p := testParquet2PC(t, "incremental-thread")
	testWriteMetadata(t, p, &types.MetadataState{
		FullRefreshCommittedIDs: []string{"chunk-1", "chunk-2"},
		DedupInserts:            boolPointer(true),
	})

	state, err := p.commitMetadata(ctx, "incremental-thread", &types.MetadataState{
		ID:           "incremental-thread",
		State:        `{"cursor":40}`,
		DedupInserts: boolPointer(false),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"chunk-1", "chunk-2"}, state.FullRefreshCommittedIDs)
	require.Equal(t, "incremental-thread", state.ID)
	require.Equal(t, `{"cursor":40}`, state.State)
	require.False(t, *state.DedupInserts)
}

func TestLoad2PCStateRejectsInvalidFinishState(t *testing.T) {
	p := testParquet2PC(t, "invalid-thread")
	require.NoError(t, p.writeObject(context.Background(), p.finishPath("invalid-thread"), []byte("{")))

	_, err := p.load2PCState(context.Background(), true)
	require.ErrorContains(t, err, "failed to unmarshal parquet 2pc finish state")
}

func testParquet2PC(t *testing.T, threadID string) *Parquet {
	t.Helper()
	return testParquet2PCWithPath(t.TempDir(), threadID)
}

func testParquet2PCWithPath(path, threadID string) *Parquet {
	return &Parquet{
		config:   &Config{Path: path},
		options:  &destination.Options{ThreadID: threadID},
		basePath: filepath.Join("namespace", "table"),
	}
}

func testWriteMetadata(t *testing.T, p *Parquet, state *types.MetadataState) {
	t.Helper()
	data, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, p.writeObject(context.Background(), p.metadataPath(), data))
}

func testWriteFinish(t *testing.T, p *Parquet, state *types.MetadataState) {
	t.Helper()
	data, _, err := finishState(state)
	require.NoError(t, err)
	require.NoError(t, p.writeObject(context.Background(), p.finishPath(p.options.ThreadID), data))
}

func testWriteStagedFile(t *testing.T, p *Parquet, path string) string {
	t.Helper()
	filePath := filepath.Join(p.localStagingPath(p.options.ThreadID), path)
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), os.ModePerm))
	require.NoError(t, os.WriteFile(filePath, []byte(path), 0o600))
	return filePath
}

func testFinalParquetFiles(t *testing.T, p *Parquet) int {
	t.Helper()
	root := filepath.Join(p.config.Path, p.basePath)
	_, err := os.Stat(root)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)

	var count int
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		if entry.IsDir() || strings.Contains(path, parquet2PCDir) {
			return nil
		}
		if filepath.Ext(path) == "."+constants.ParquetFileExt {
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

func requirePathExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.NoError(t, err)
}

func requirePathNotExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func boolPointer(value bool) *bool {
	return &value
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
