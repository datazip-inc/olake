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
		{
			name: "returns full refresh commit marker ids",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "thread-1")
				require.NoError(t, p.writeCommitMarker(ctx, nil))

				p.options.ThreadID = "thread-2"
				require.NoError(t, p.writeCommitMarker(ctx, nil))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "thread-1"))
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "thread-2"))
			},
		},
		{
			name: "returns latest metadata state",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "cdc-thread-old")
				require.NoError(t, p.writeCommitMarker(ctx, &types.MetadataState{
					ID:    "cdc-thread-old",
					State: `{"lsn":"1/1"}`,
				}))

				p.options.ThreadID = "cdc-thread-new"
				require.NoError(t, p.writeCommitMarker(ctx, &types.MetadataState{
					ID:    "cdc-thread-new",
					State: `{"lsn":"1/2"}`,
				}))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.Equal(t, "cdc-thread-new", state.ID)
				require.Equal(t, `{"lsn":"1/2"}`, state.State)
				require.Empty(t, state.FullRefreshCommittedIDs)
			},
		},
		{
			name: "promotes committed staging before returning state",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "committed-thread")
				stagedFile := testWriteStagedFile(t, p, "bucket_1/data.parquet")
				requireFileExists(t, stagedFile)
				require.NoError(t, p.writeCommitMarker(ctx, nil))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "committed-thread"))

				requireFileExists(t, filepath.Join(p.config.Path, p.basePath, "bucket_1", "data.parquet"))
				requireFileNotExists(t, stagedFile)
				requireDirNotExists(t, p.localStagingPath("committed-thread"))
			},
		},
		{
			name: "deletes uncommitted staging without touching final files",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "failed-thread")
				finalFile := testWriteFinalFile(t, p, "existing.parquet")
				stagedFile := testWriteStagedFile(t, p, "failed.parquet")

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.Nil(t, state)

				requireFileExists(t, finalFile)
				requireFileNotExists(t, stagedFile)
				requireDirNotExists(t, p.localStagingPath("failed-thread"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestMetadataStateWrapsIncrementalPayload(t *testing.T) {
	p := testParquet2PC(t, "incremental-thread")

	state, err := p.metadataState(map[string]any{"id": 10})
	require.NoError(t, err)
	require.Equal(t, "incremental-thread", state.ID)
	require.Equal(t, `{"id":10}`, state.State)
}

func TestCloseWritesCommitMarkerWithMetadataState(t *testing.T) {
	ctx := context.Background()
	stream := testConfiguredStream()
	p := &Parquet{config: &Config{Path: t.TempDir()}}
	options := &destination.Options{ThreadID: "incremental-thread"}

	_, state, err := p.Setup(ctx, stream, nil, options)
	require.NoError(t, err)
	require.Nil(t, state)

	err = p.Write(ctx, []types.RawRecord{
		types.CreateRawRecord(
			map[string]any{"id": 1},
			map[string]any{
				constants.OlakeID:        "row-1",
				constants.OlakeTimestamp: time.Now().UTC(),
				constants.OpType:         "i",
			},
		),
	})
	require.NoError(t, err)

	metadataState := &types.MetadataState{
		ID:    "incremental-thread",
		State: `{"cursor":1}`,
	}
	require.NoError(t, p.Close(ctx, metadataState))

	commitPath := filepath.Join(p.local2PCPath(), p.commitMarkerName("incremental-thread"))
	commitData, err := os.ReadFile(commitPath)
	require.NoError(t, err)

	var markerState types.MetadataState
	require.NoError(t, json.Unmarshal(commitData, &markerState))
	require.Equal(t, "incremental-thread", markerState.ID)
	require.Equal(t, `{"cursor":1}`, markerState.State)
	requireFileNotExists(t, p.localStagingPath("incremental-thread"))
	require.Equal(t, 1, testFinalParquetFiles(t, p))
}

func TestCloseWritesFullRefreshCommitMarker(t *testing.T) {
	ctx := context.Background()
	stream := testConfiguredStream()
	p := &Parquet{config: &Config{Path: t.TempDir()}}
	options := &destination.Options{ThreadID: "full-refresh-thread"}

	_, state, err := p.Setup(ctx, stream, nil, options)
	require.NoError(t, err)
	require.Nil(t, state)

	err = p.Write(ctx, []types.RawRecord{
		types.CreateRawRecord(
			map[string]any{"id": 1},
			map[string]any{
				constants.OlakeID:        "row-1",
				constants.OlakeTimestamp: time.Now().UTC(),
				constants.OpType:         "r",
			},
		),
	})
	require.NoError(t, err)
	require.NoError(t, p.Close(ctx, nil))

	commitPath := filepath.Join(p.local2PCPath(), p.commitMarkerName("full-refresh-thread"))
	commitData, err := os.ReadFile(commitPath)
	require.NoError(t, err)
	require.Empty(t, commitData)
	requireFileNotExists(t, p.localStagingPath("full-refresh-thread"))
	require.Equal(t, 1, testFinalParquetFiles(t, p))
}

func TestCloseWritesMetadataOnlyCommitMarker(t *testing.T) {
	ctx := context.Background()
	stream := testConfiguredStream()
	p := &Parquet{config: &Config{Path: t.TempDir()}}
	options := &destination.Options{ThreadID: "cdc-thread"}

	_, state, err := p.Setup(ctx, stream, nil, options)
	require.NoError(t, err)
	require.Nil(t, state)

	metadataState := &types.MetadataState{
		ID:    "cdc-thread",
		State: `{"lsn":"1/1"}`,
	}
	require.NoError(t, p.Close(ctx, metadataState))

	state, err = p.load2PCState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, "cdc-thread", state.ID)
	require.Equal(t, `{"lsn":"1/1"}`, state.State)

	commitPath := filepath.Join(p.local2PCPath(), p.commitMarkerName("cdc-thread"))
	commitData, err := os.ReadFile(commitPath)
	require.NoError(t, err)

	var markerState types.MetadataState
	require.NoError(t, json.Unmarshal(commitData, &markerState))
	require.Equal(t, "cdc-thread", markerState.ID)
	require.Equal(t, `{"lsn":"1/1"}`, markerState.State)
	require.Equal(t, 0, testFinalParquetFiles(t, p))
}

func testParquet2PC(t *testing.T, threadID string) *Parquet {
	t.Helper()
	return &Parquet{
		config: &Config{Path: t.TempDir()},
		options: &destination.Options{
			ThreadID: threadID,
		},
		basePath: filepath.Join("namespace", "table"),
	}
}

func testWriteFinalFile(t *testing.T, p *Parquet, path string) string {
	t.Helper()
	filePath := filepath.Join(p.config.Path, p.basePath, path)
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), os.ModePerm))
	require.NoError(t, os.WriteFile(filePath, []byte(path), 0o600))
	return filePath
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
	var count int
	require.NoError(t, filepath.WalkDir(filepath.Join(p.config.Path, p.basePath), func(path string, d os.DirEntry, err error) error {
		require.NoError(t, err)
		if d.IsDir() {
			return nil
		}
		if strings.Contains(path, parquet2PCDir) {
			return nil
		}
		if filepath.Ext(path) == "."+constants.ParquetFileExt {
			count++
		}
		return nil
	}))
	return count
}

func requireFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.NoError(t, err)
}

func requireFileNotExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func requireDirNotExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
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
