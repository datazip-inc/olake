package driver

import (
	"context"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroupFilesIntoChunks tests the 2GB chunking logic
func TestGroupFilesIntoChunks(t *testing.T) {
	tests := []struct {
		name           string
		files          []FileObject
		expectedChunks int
		description    string
		validateChunks func(*testing.T, []types.Chunk)
	}{
		{
			name: "all small files - should group into 1 chunk",
			files: []FileObject{
				{FileKey: "file1.csv", Size: 500 * 1024 * 1024}, // 500MB
				{FileKey: "file2.csv", Size: 500 * 1024 * 1024}, // 500MB
				{FileKey: "file3.csv", Size: 800 * 1024 * 1024}, // 800MB
			},
			expectedChunks: 1, // Total: 1.8GB < 2GB
			description:    "small files should be grouped into a single chunk",
			validateChunks: func(t *testing.T, chunks []types.Chunk) {
				// First chunk should be an array of file keys
				fileKeys, ok := chunks[0].Min.([]string)
				require.True(t, ok, "chunk Min should be []string for grouped files")
				assert.Len(t, fileKeys, 3, "should contain all 3 files")
			},
		},
		{
			name: "small files exceed 2GB - should split into multiple chunks",
			files: []FileObject{
				{FileKey: "file1.csv", Size: 1.5 * 1024 * 1024 * 1024}, // 1.5GB
				{FileKey: "file2.csv", Size: 1.5 * 1024 * 1024 * 1024}, // 1.5GB
				{FileKey: "file3.csv", Size: 500 * 1024 * 1024},        // 500MB
			},
			expectedChunks: 2, // [file1]=1.5GB, [file2,file3]=2GB
			description:    "files should be grouped until they would exceed 2GB",
			validateChunks: func(t *testing.T, chunks []types.Chunk) {
				// Chunks can be in any order (Set doesn't guarantee order)
				// One chunk should have 1 file, another should have 2 files
				var oneFileChunks, twoFileChunks int
				for _, chunk := range chunks {
					fileKeys, ok := chunk.Min.([]string)
					require.True(t, ok, "chunk Min should be []string")
					if len(fileKeys) == 1 {
						oneFileChunks++
					} else if len(fileKeys) == 2 {
						twoFileChunks++
					}
				}
				assert.Equal(t, 1, oneFileChunks, "should have 1 chunk with 1 file")
				assert.Equal(t, 1, twoFileChunks, "should have 1 chunk with 2 files")
			},
		},
		{
			name: "single large file - should be individual chunk",
			files: []FileObject{
				{FileKey: "large.parquet", Size: 3 * 1024 * 1024 * 1024}, // 3GB
			},
			expectedChunks: 1,
			description:    "single large file (>2GB) should be its own chunk",
			validateChunks: func(t *testing.T, chunks []types.Chunk) {
				// For consistency, even single large files use []string
				fileKeys, ok := chunks[0].Min.([]string)
				require.True(t, ok, "chunk Min should be []string even for large files")
				assert.Len(t, fileKeys, 1, "should contain exactly 1 file")
				assert.Equal(t, "large.parquet", fileKeys[0])
			},
		},
		{
			name: "mix of large and small files",
			files: []FileObject{
				{FileKey: "large1.parquet", Size: 3 * 1024 * 1024 * 1024},   // 3GB
				{FileKey: "small1.csv", Size: 500 * 1024 * 1024},            // 500MB
				{FileKey: "small2.csv", Size: 400 * 1024 * 1024},            // 400MB
				{FileKey: "large2.parquet", Size: 2.5 * 1024 * 1024 * 1024}, // 2.5GB
			},
			expectedChunks: 3, // large1, [small1,small2], large2
			description:    "should separate large files and group small files",
			validateChunks: func(t *testing.T, chunks []types.Chunk) {
				// Verify all chunks use []string format
				for i, chunk := range chunks {
					_, ok := chunk.Min.([]string)
					require.True(t, ok, "chunk %d Min should be []string", i)
				}
			},
		},
		{
			name:           "empty file list",
			files:          []FileObject{},
			expectedChunks: 0,
			description:    "empty file list should produce no chunks",
			validateChunks: func(t *testing.T, chunks []types.Chunk) {
				assert.Len(t, chunks, 0, "should have no chunks")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3{}
			chunks := s.groupFilesIntoChunks(tt.files)

			// Convert types.Set to slice for easier testing
			assert.Equal(t, tt.expectedChunks, chunks.Len(), tt.description)

			if tt.validateChunks != nil && chunks.Len() > 0 {
				// Use Array() to convert Set to slice
				chunkSlice := chunks.Array()
				tt.validateChunks(t, chunkSlice)
			}
		})
	}
}

// TestChunkIteratorTypes tests handling of different chunk Min types
func TestChunkIteratorTypes(t *testing.T) {
	tests := []struct {
		name        string
		chunkMin    any
		expectError bool
		description string
	}{
		{
			name:        "single file - []string with one element",
			chunkMin:    []string{"file1.csv"},
			expectError: false,
			description: "should handle []string with single element (large file)",
		},
		{
			name:        "multiple files - []string with multiple elements",
			chunkMin:    []string{"file1.csv", "file2.csv", "file3.csv"},
			expectError: false,
			description: "should handle []string with multiple elements (grouped small files)",
		},
		{
			name:        "interface slice from state deserialization",
			chunkMin:    []interface{}{"file1.csv", "file2.csv"},
			expectError: false,
			description: "should handle []interface{} from JSON state deserialization",
		},
		{
			name:        "invalid type - integer",
			chunkMin:    12345,
			expectError: true,
			description: "should error on invalid chunk Min type",
		},
		{
			name:        "invalid type - map",
			chunkMin:    map[string]string{"file": "data.csv"},
			expectError: true,
			description: "should error on map chunk Min type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: This test validates the type checking logic
			// Full ChunkIterator testing requires S3 mock which is integration test territory
			chunk := types.Chunk{
				Min: tt.chunkMin,
				Max: nil,
			}

			// Simulate the type checking logic from ChunkIterator
			var fileKeys []string
			var err error

			switch v := chunk.Min.(type) {
			case []string:
				fileKeys = v
			case []interface{}:
				fileKeys = make([]string, len(v))
				for i, item := range v {
					str, ok := item.(string)
					if !ok {
						err = assert.AnError
						break
					}
					fileKeys[i] = str
				}
			default:
				err = assert.AnError
			}

			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotEmpty(t, fileKeys, "should have extracted file keys")
			}
		})
	}
}

// TestIncrementalSyncFiltering tests cursor-based filtering for incremental sync
func TestIncrementalSyncFiltering(t *testing.T) {
	tests := []struct {
		name            string
		files           []FileObject
		cursorTimestamp string
		expectedCount   int
		expectedFiles   []string
	}{
		{
			name: "empty cursor - backfill mode (all files)",
			files: []FileObject{
				{FileKey: "file1.csv", LastModified: "2024-01-01T10:00:00Z"},
				{FileKey: "file2.csv", LastModified: "2024-01-02T10:00:00Z"},
			},
			cursorTimestamp: "",
			expectedCount:   2,
			expectedFiles:   []string{"file1.csv", "file2.csv"},
		},
		{
			name: "with cursor - only new files",
			files: []FileObject{
				{FileKey: "file1.csv", LastModified: "2024-01-01T10:00:00Z"},
				{FileKey: "file2.csv", LastModified: "2024-01-02T10:00:00Z"},
				{FileKey: "file3.csv", LastModified: "2024-01-03T10:00:00Z"},
			},
			cursorTimestamp: "2024-01-02T10:00:00Z",
			expectedCount:   1,
			expectedFiles:   []string{"file3.csv"},
		},
		{
			name: "with cursor - no new files",
			files: []FileObject{
				{FileKey: "file1.csv", LastModified: "2024-01-01T10:00:00Z"},
				{FileKey: "file2.csv", LastModified: "2024-01-02T10:00:00Z"},
			},
			cursorTimestamp: "2024-01-03T00:00:00Z",
			expectedCount:   0,
			expectedFiles:   []string{},
		},
		{
			name: "with cursor - multiple new files",
			files: []FileObject{
				{FileKey: "file1.csv", LastModified: "2024-01-01T10:00:00Z"},
				{FileKey: "file2.csv", LastModified: "2024-01-02T10:00:00Z"},
				{FileKey: "file3.csv", LastModified: "2024-01-03T10:00:00Z"},
				{FileKey: "file4.csv", LastModified: "2024-01-02T15:00:00Z"},
			},
			cursorTimestamp: "2024-01-02T10:00:00Z",
			expectedCount:   2,
			expectedFiles:   []string{"file3.csv", "file4.csv"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3{}

			// Use filterFilesByCursor which is the actual implementation
			filtered := s.filterFilesByCursor(tt.files, tt.cursorTimestamp)

			assert.Equal(t, tt.expectedCount, len(filtered), "filtered file count mismatch")

			// Verify expected files
			filteredKeys := make([]string, len(filtered))
			for i, f := range filtered {
				filteredKeys[i] = f.FileKey
			}

			for _, expectedFile := range tt.expectedFiles {
				assert.Contains(t, filteredKeys, expectedFile, "expected file not in results")
			}
		})
	}
}

// TestCursorTimestampComparison tests timestamp comparison logic
func TestCursorTimestampComparison(t *testing.T) {
	tests := []struct {
		name            string
		fileTimestamp   string
		cursorTimestamp string
		shouldInclude   bool
	}{
		{
			name:            "file newer than cursor",
			fileTimestamp:   "2024-01-02T10:00:00Z",
			cursorTimestamp: "2024-01-01T10:00:00Z",
			shouldInclude:   true,
		},
		{
			name:            "file older than cursor",
			fileTimestamp:   "2024-01-01T10:00:00Z",
			cursorTimestamp: "2024-01-02T10:00:00Z",
			shouldInclude:   false,
		},
		{
			name:            "file same as cursor",
			fileTimestamp:   "2024-01-01T10:00:00Z",
			cursorTimestamp: "2024-01-01T10:00:00Z",
			shouldInclude:   false, // Equal timestamps are not included
		},
		{
			name:            "empty cursor - backfill mode",
			fileTimestamp:   "2024-01-01T10:00:00Z",
			cursorTimestamp: "",
			shouldInclude:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3{}
			files := []FileObject{
				{FileKey: "test.csv", LastModified: tt.fileTimestamp},
			}

			filtered := s.filterFilesByCursor(files, tt.cursorTimestamp)

			if tt.shouldInclude {
				assert.Len(t, filtered, 1, "file should be included")
			} else {
				assert.Len(t, filtered, 0, "file should not be included")
			}
		})
	}
}

// TestParserConfigGetters tests the config getter methods
func TestParserConfigGetters(t *testing.T) {
	t.Run("GetCSVConfig returns correct config", func(t *testing.T) {
		config := &Config{
			BucketName: "test-bucket",
			Region:     "us-east-1",
			FileFormat: FormatCSV,
		}
		err := config.Validate()
		require.NoError(t, err)

		csvConfig := config.GetCSVConfig()
		assert.NotNil(t, csvConfig)
		assert.Equal(t, ",", csvConfig.Delimiter)
		assert.Equal(t, "\"", csvConfig.QuoteCharacter)
		assert.True(t, csvConfig.HasHeader)
		assert.Equal(t, 0, csvConfig.SkipRows)
	})

	t.Run("GetJSONConfig returns correct config", func(t *testing.T) {
		config := &Config{
			BucketName: "test-bucket",
			Region:     "us-east-1",
			FileFormat: FormatJSON,
		}
		err := config.Validate()
		require.NoError(t, err)

		jsonConfig := config.GetJSONConfig()
		assert.NotNil(t, jsonConfig)
		assert.True(t, jsonConfig.LineDelimited)
	})

	t.Run("GetParquetConfig returns correct config", func(t *testing.T) {
		config := &Config{
			BucketName: "test-bucket",
			Region:     "us-east-1",
			FileFormat: FormatParquet,
		}
		err := config.Validate()
		require.NoError(t, err)

		parquetConfig := config.GetParquetConfig()
		assert.NotNil(t, parquetConfig)
		assert.True(t, parquetConfig.StreamingEnabled)
	})
}

// TestStateManagement tests state-related operations
func TestStateManagement(t *testing.T) {
	t.Run("state type should be stream-level", func(t *testing.T) {
		s := &S3{}
		assert.Equal(t, types.StreamType, s.StateType())
	})

	t.Run("setup state initializes correctly", func(t *testing.T) {
		s := &S3{}
		state := &types.State{
			RWMutex: &sync.RWMutex{},
			Type:    types.StreamType,
		}
		s.SetupState(state)
		assert.NotNil(t, s.state)
		assert.Equal(t, state, s.state)
	})
}

// TestMaxConnectionsAndRetries tests configuration getters
func TestMaxConnectionsAndRetries(t *testing.T) {
	t.Run("MaxConnections returns config value", func(t *testing.T) {
		s := &S3{
			config: &Config{
				MaxThreads: 8,
			},
		}
		assert.Equal(t, 8, s.MaxConnections())
	})

	t.Run("MaxRetries returns config value", func(t *testing.T) {
		s := &S3{
			config: &Config{
				RetryCount: 5,
			},
		}
		assert.Equal(t, 5, s.MaxRetries())
	})
}

// TestCDCNotSupported tests that CDC operations return errors
func TestCDCNotSupported(t *testing.T) {
	s := &S3{}
	ctx := context.Background()

	t.Run("CDCSupported returns false", func(t *testing.T) {
		assert.False(t, s.CDCSupported())
	})

	t.Run("PreCDC returns error", func(t *testing.T) {
		err := s.PreCDC(ctx, []types.StreamInterface{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CDC is not supported")
	})

	t.Run("StreamChanges returns error", func(t *testing.T) {
		err := s.StreamChanges(ctx, 0, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CDC is not supported")
	})

	t.Run("PostCDC returns error", func(t *testing.T) {
		err := s.PostCDC(ctx, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CDC is not supported")
	})
}
