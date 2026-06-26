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
		// full refresh markers have no metadata, so setup recovers committed thread ids from marker paths
		{
			name: "returns full refresh completed marker ids",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "thread-1")
				require.NoError(t, p.writeCompletedMarker(ctx, nil))

				p.options.ThreadID = "thread-2"
				require.NoError(t, p.writeCompletedMarker(ctx, nil))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "thread-1"))
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "thread-2"))
			},
		},
		// CDC/incremental markers can persist metadata state, so setup selects the newest marker
		{
			name: "returns latest metadata state",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "cdc-thread-old")
				require.NoError(t, p.writeCompletedMarker(ctx, &types.MetadataState{
					ID:    "cdc-thread-old",
					State: `{"lsn":"1/1"}`,
				}))
				oldModTime := time.Now().Add(-time.Minute)
				require.NoError(t, os.Chtimes(p.completedMarkerPath("cdc-thread-old"), oldModTime, oldModTime))

				p.options.ThreadID = "cdc-thread-new"
				require.NoError(t, p.writeCompletedMarker(ctx, &types.MetadataState{
					ID:    "cdc-thread-new",
					State: `{"lsn":"1/2"}`,
				}))
				newModTime := time.Now()
				require.NoError(t, os.Chtimes(p.completedMarkerPath("cdc-thread-new"), newModTime, newModTime))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.Equal(t, "cdc-thread-new", state.ID)
				require.Equal(t, `{"lsn":"1/2"}`, state.State)
				require.Empty(t, state.FullRefreshCommittedIDs)
			},
		},
		// completed staging is rolled forward into final table paths before setup returns
		{
			name: "promotes completed staging before returning state",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "completed-thread")
				stagedFile := testWriteStagedFile(t, p, "bucket_1/data.parquet")
				requireFileExists(t, stagedFile)
				require.NoError(t, p.writeCompletedMarker(ctx, nil))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "completed-thread"))

				requireFileExists(t, filepath.Join(p.config.Path, p.basePath, "bucket_1", "data.parquet"))
				requireFileNotExists(t, stagedFile)
				requireFileExists(t, p.completedMarkerPath("completed-thread"))
				require.Equal(t, 0, testStagedParquetFiles(t, p, "completed-thread"))
			},
		},
		// staging without a completed marker belongs to an interrupted attempt and is removed
		{
			name: "deletes staging without completed marker",
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

// TestMetadataStateWrapsIncrementalPayload verifies raw cursor payloads are wrapped for recovery.
func TestMetadataStateWrapsIncrementalPayload(t *testing.T) {
	p := testParquet2PC(t, "incremental-thread")

	state, err := p.metadataState(map[string]any{"id": 10})
	require.NoError(t, err)
	require.Equal(t, "incremental-thread", state.ID)
	require.Equal(t, `{"id":10}`, state.State)
}

// TestCloseWritesCompletedMarkerWithMetadataState verifies close stores cursor metadata with data.
func TestCloseWritesCompletedMarkerWithMetadataState(t *testing.T) {
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

	markerData, err := os.ReadFile(p.completedMarkerPath("incremental-thread"))
	require.NoError(t, err)

	var markerState types.MetadataState
	require.NoError(t, json.Unmarshal(markerData, &markerState))
	require.Equal(t, "incremental-thread", markerState.ID)
	require.Equal(t, `{"cursor":1}`, markerState.State)
	requireFileExists(t, p.completedMarkerPath("incremental-thread"))
	require.Equal(t, 0, testStagedParquetFiles(t, p, "incremental-thread"))
	require.Equal(t, 1, testFinalParquetFiles(t, p))
}

// TestCloseWritesFullRefreshCompletedMarker verifies full refresh close writes an empty marker.
func TestCloseWritesFullRefreshCompletedMarker(t *testing.T) {
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

	markerData, err := os.ReadFile(p.completedMarkerPath("full-refresh-thread"))
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(markerData))
	require.Equal(t, 0, testStagedParquetFiles(t, p, "full-refresh-thread"))
	require.Equal(t, 1, testFinalParquetFiles(t, p))
}

// TestCloseWritesMetadataOnlyCompletedMarker verifies no-op syncs still commit metadata state.
func TestCloseWritesMetadataOnlyCompletedMarker(t *testing.T) {
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

	markerData, err := os.ReadFile(p.completedMarkerPath("cdc-thread"))
	require.NoError(t, err)

	var markerState types.MetadataState
	require.NoError(t, json.Unmarshal(markerData, &markerState))
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

func testStagedParquetFiles(t *testing.T, p *Parquet, threadID string) int {
	t.Helper()
	var count int
	stagingPath := p.localStagingPath(threadID)
	require.NoError(t, filepath.WalkDir(stagingPath, func(path string, d os.DirEntry, err error) error {
		require.NoError(t, err)
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) == "."+constants.ParquetFileExt {
			count++
		}
		return nil
	}))
	return count
}

func (p *Parquet) completedMarkerPath(threadID string) string {
	return filepath.Join(p.local2PCPath(), p.completedMarkerName(threadID))
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
