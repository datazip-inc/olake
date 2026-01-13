package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExtractStreamName tests the folder grouping logic (always level 1)
func TestExtractStreamName(t *testing.T) {
	tests := []struct {
		name               string
		pathPrefix         string
		fileKey            string
		expectedStreamName string
	}{
		{
			name:               "extract first folder - multiple levels",
			pathPrefix:         "",
			fileKey:            "users/2024-01-01/data.csv",
			expectedStreamName: "users",
		},
		{
			name:               "with path prefix - remove prefix and extract first folder",
			pathPrefix:         "data/raw",
			fileKey:            "data/raw/users/2024-01-01/data.csv",
			expectedStreamName: "users",
		},
		{
			name:               "file at root level - no folders",
			pathPrefix:         "",
			fileKey:            "data.csv",
			expectedStreamName: "data.csv",
		},
		{
			name:               "single folder level",
			pathPrefix:         "",
			fileKey:            "users/data.csv",
			expectedStreamName: "users",
		},
		{
			name:               "deeply nested - only first folder",
			pathPrefix:         "",
			fileKey:            "orders/2024/01/15/file.csv",
			expectedStreamName: "orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3{
				config: &Config{
					PathPrefix: tt.pathPrefix,
				},
			}

			streamName := s.extractStreamName(tt.fileKey)
			assert.Equal(t, tt.expectedStreamName, streamName)
		})
	}
}

// TestConfigValidation tests configuration validation
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				FileFormat:      FormatCSV,
			},
			expectError: false,
		},
		{
			name: "missing bucket name",
			config: Config{
				Region:          "us-east-1",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				FileFormat:      FormatCSV,
			},
			expectError: true,
			errorMsg:    "bucket_name is required",
		},
		{
			name: "missing credentials - should pass (uses default credential chain)",
			config: Config{
				BucketName: "test-bucket",
				Region:     "us-east-1",
				FileFormat: FormatCSV,
			},
			expectError: false, // Changed: now credentials are optional
		},
		{
			name: "partial credentials - access key only",
			config: Config{
				BucketName:  "test-bucket",
				Region:      "us-east-1",
				AccessKeyID: "test-key",
				// Missing SecretAccessKey
				FileFormat: FormatCSV,
			},
			expectError: true,
			errorMsg:    "access_key_id and secret_access_key must be provided together",
		},
		{
			name: "partial credentials - secret key only",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				SecretAccessKey: "test-secret",
				// Missing AccessKeyID
				FileFormat: FormatCSV,
			},
			expectError: true,
			errorMsg:    "access_key_id and secret_access_key must be provided together",
		},
		{
			name: "invalid file format",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				FileFormat:      "invalid",
			},
			expectError: true,
			errorMsg:    "invalid file_format",
		},
		{
			name: "defaults applied",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
				FileFormat:      FormatCSV,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				// Check defaults are applied
				assert.NotZero(t, tt.config.MaxThreads)
			}
		})
	}
}

// TestMatchesFileFormat tests file format matching
func TestMatchesFileFormat(t *testing.T) {
	tests := []struct {
		name        string
		fileFormat  FileFormat
		compression CompressionType
		fileKey     string
		expected    bool
	}{
		{
			name:        "CSV without compression",
			fileFormat:  FormatCSV,
			compression: CompressionNone,
			fileKey:     "data.csv",
			expected:    true,
		},
		{
			name:        "CSV with gzip compression",
			fileFormat:  FormatCSV,
			compression: CompressionGzip,
			fileKey:     "data.csv.gz",
			expected:    true,
		},
		{
			name:        "JSON line delimited",
			fileFormat:  FormatJSON,
			compression: CompressionNone,
			fileKey:     "data.jsonl",
			expected:    true,
		},
		{
			name:        "JSON with gzip",
			fileFormat:  FormatJSON,
			compression: CompressionGzip,
			fileKey:     "data.json.gz",
			expected:    true,
		},
		{
			name:        "Parquet",
			fileFormat:  FormatParquet,
			compression: CompressionNone,
			fileKey:     "data.parquet",
			expected:    true,
		},
		{
			name:        "wrong format",
			fileFormat:  FormatCSV,
			compression: CompressionNone,
			fileKey:     "data.json",
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3{
				config: &Config{
					FileFormat:  tt.fileFormat,
					Compression: tt.compression,
				},
			}

			result := s.matchesFileFormat(tt.fileKey)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFetchMaxCursorValues tests cursor value fetching
func TestFetchMaxCursorValues(t *testing.T) {
	s := &S3{
		discoveredFiles: map[string][]FileObject{
			"users": {
				{FileKey: "users/file1.csv", LastModified: "2024-01-01T10:00:00Z"},
				{FileKey: "users/file2.csv", LastModified: "2024-01-02T10:00:00Z"},
				{FileKey: "users/file3.csv", LastModified: "2024-01-01T15:00:00Z"},
			},
			"orders": {
				{FileKey: "orders/file1.csv", LastModified: "2024-01-03T10:00:00Z"},
			},
		},
	}

	tests := []struct {
		name           string
		streamName     string
		expectedCursor string
	}{
		{
			name:           "multiple files - returns max",
			streamName:     "users",
			expectedCursor: "2024-01-02T10:00:00Z",
		},
		{
			name:           "single file",
			streamName:     "orders",
			expectedCursor: "2024-01-03T10:00:00Z",
		},
		{
			name:           "non-existent stream",
			streamName:     "products",
			expectedCursor: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := types.NewStream(tt.streamName, "s3", nil)
			configuredStream := stream.Wrap(0)

			ctx := context.Background()
			cursor, _, err := s.FetchMaxCursorValues(ctx, configuredStream)
			require.NoError(t, err)

			if tt.expectedCursor == "" {
				assert.Nil(t, cursor)
			} else {
				assert.Equal(t, tt.expectedCursor, cursor)
			}
		})
	}
}

// TestFilterFilesByCursor tests the unified cursor-based filtering logic
func TestFilterFilesByCursor(t *testing.T) {
	s := &S3{}

	files := []FileObject{
		{FileKey: "file1.csv", LastModified: "2024-01-01T10:00:00Z"},
		{FileKey: "file2.csv", LastModified: "2024-01-02T10:00:00Z"},
		{FileKey: "file3.csv", LastModified: "2024-01-03T10:00:00Z"},
		{FileKey: "file4.csv", LastModified: "2024-01-02T15:00:00Z"},
	}

	tests := []struct {
		name            string
		cursorTimestamp string
		expectedCount   int
		expectedFiles   []string
	}{
		{
			name:            "empty cursor - backfill mode (all files)",
			cursorTimestamp: "",
			expectedCount:   4,
			expectedFiles:   []string{"file1.csv", "file2.csv", "file3.csv", "file4.csv"},
		},
		{
			name:            "cursor in middle - incremental mode",
			cursorTimestamp: "2024-01-02T10:00:00Z",
			expectedCount:   2,
			expectedFiles:   []string{"file3.csv", "file4.csv"},
		},
		{
			name:            "cursor after all files - no files to process",
			cursorTimestamp: "2024-01-04T00:00:00Z",
			expectedCount:   0,
			expectedFiles:   []string{},
		},
		{
			name:            "cursor before all files - all files",
			cursorTimestamp: "2024-01-01T00:00:00Z",
			expectedCount:   4,
			expectedFiles:   []string{"file1.csv", "file2.csv", "file3.csv", "file4.csv"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filtered := s.filterFilesByCursor(files, tt.cursorTimestamp)

			assert.Equal(t, tt.expectedCount, len(filtered), "filtered file count mismatch")

			// Verify expected files are in the filtered result
			filteredKeys := make([]string, len(filtered))
			for i, f := range filtered {
				filteredKeys[i] = f.FileKey
			}

			for _, expectedFile := range tt.expectedFiles {
				assert.Contains(t, filteredKeys, expectedFile, "expected file not found in filtered results")
			}
		})
	}
}
