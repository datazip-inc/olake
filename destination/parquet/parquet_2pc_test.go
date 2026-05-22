package parquet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
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
			name: "returns committed ids",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "thread-1")
				require.NoError(t, p.writeCommitMarker(ctx, testDataFiles(p, "part/a.parquet"), nil))

				p.options.ThreadID = "thread-2"
				require.NoError(t, p.writeCommitMarker(ctx, testDataFiles(p, "part/b.parquet"), nil))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "thread-1"))
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "thread-2"))
			},
		},
		{
			name: "falls back to commit marker state",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "cdc-thread")
				metadataState := &types.MetadataState{
					ID:    "cdc-thread",
					State: `{"lsn":"1/1"}`,
				}
				require.NoError(t, p.writeCommitMarker(ctx, testDataFiles(p, "cdc.parquet"), metadataState))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.Equal(t, "cdc-thread", state.ID)
				require.Equal(t, `{"lsn":"1/1"}`, state.State)
				require.Empty(t, state.FullRefreshCommittedIDs)
			},
		},
		{
			name: "cleans only prepared uncommitted files",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "failed-thread")
				legacyFile := testWriteFile(t, p, "legacy.parquet")
				failedFile := testWriteFile(t, p, "failed.parquet")
				require.NoError(t, p.writePrepareMarker(ctx, testDataFiles(p, "failed.parquet"), nil))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.Nil(t, state)

				requireFileExists(t, legacyFile)
				requireFileNotExists(t, failedFile)
			},
		},
		{
			name: "keeps prepared files when commit marker exists",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "committed-thread")
				committedFile := testWriteFile(t, p, "committed.parquet")
				files := testDataFiles(p, "committed.parquet")
				require.NoError(t, p.writePrepareMarker(ctx, files, nil))
				require.NoError(t, p.writeCommitMarker(ctx, files, nil))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.NotNil(t, state)
				require.True(t, slices.Contains(state.FullRefreshCommittedIDs, "committed-thread"))

				requireFileExists(t, committedFile)
				requireFileNotExists(t, testPrepareMarkerPath(p, "committed-thread"))
			},
		},
		{
			name: "cleans prepared files with metadata without commit marker",
			run: func(t *testing.T) {
				p := testParquet2PC(t, "incremental-thread")
				failedFile := testWriteFile(t, p, "failed.parquet")
				metadataState := &types.MetadataState{
					ID:    "incremental-thread",
					State: `{"cursor":1}`,
				}
				files := testDataFiles(p, "failed.parquet")
				require.NoError(t, p.writePrepareMarker(ctx, files, metadataState))

				state, err := p.load2PCState(ctx)
				require.NoError(t, err)
				require.Nil(t, state)

				requireFileNotExists(t, failedFile)
				requireFileNotExists(t, testPrepareMarkerPath(p, "incremental-thread"))
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

	commitPath := filepath.Join(p.local2PCPath(), parquet2PCCommitsDir, p.markerFileName("incremental-thread"))
	commitData, err := os.ReadFile(commitPath)
	require.NoError(t, err)

	var marker parquet2PCMarker
	require.NoError(t, json.Unmarshal(commitData, &marker))
	require.Equal(t, "incremental-thread", marker.ThreadID)
	require.Len(t, marker.Files, 1)
	require.NotNil(t, marker.MetadataState)
	require.Equal(t, `{"cursor":1}`, marker.MetadataState.State)

	_, err = os.Stat(filepath.Join(p.local2PCPath(), parquet2PCPrepareDir, p.markerFileName("incremental-thread")))
	require.True(t, os.IsNotExist(err))
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

	commitPath := filepath.Join(p.local2PCPath(), parquet2PCCommitsDir, p.markerFileName("cdc-thread"))
	commitData, err := os.ReadFile(commitPath)
	require.NoError(t, err)

	var marker parquet2PCMarker
	require.NoError(t, json.Unmarshal(commitData, &marker))
	require.Equal(t, "cdc-thread", marker.ThreadID)
	require.Empty(t, marker.Files)
	require.NotNil(t, marker.MetadataState)
	require.Equal(t, `{"lsn":"1/1"}`, marker.MetadataState.State)
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

func testDataFiles(p *Parquet, paths ...string) []parquetDataFile {
	files := make([]parquetDataFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, parquetDataFile{Path: filepath.Join(p.basePath, path)})
	}
	return files
}

func testWriteFile(t *testing.T, p *Parquet, path string) string {
	t.Helper()
	filePath := filepath.Join(p.config.Path, p.basePath, path)
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), os.ModePerm))
	require.NoError(t, os.WriteFile(filePath, []byte(path), 0o600))
	return filePath
}

func testPrepareMarkerPath(p *Parquet, threadID string) string {
	return filepath.Join(p.local2PCPath(), parquet2PCPrepareDir, p.markerFileName(threadID))
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
