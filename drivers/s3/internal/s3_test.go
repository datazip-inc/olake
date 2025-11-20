package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExtractStreamName tests the folder grouping logic
func TestExtractStreamName(t *testing.T) {
	tests := []struct {
		name                  string
		pathPrefix            string
		streamGroupingEnabled bool
		streamGroupingLevel   int
		fileKey               string
		expectedStreamName    string
	}{
		{
			name:                  "grouping disabled - file is stream",
			pathPrefix:            "",
			streamGroupingEnabled: false,
			streamGroupingLevel:   1,
			fileKey:               "users/2024-01-01/data.csv",
			expectedStreamName:    "users/2024-01-01/data.csv",
		},
		{
			name:                  "grouping level 1 - extract first folder",
			pathPrefix:            "",
			streamGroupingEnabled: true,
			streamGroupingLevel:   1,
			fileKey:               "users/2024-01-01/data.csv",
			expectedStreamName:    "users",
		},
		{
			name:                  "grouping level 2 - extract two folders",
			pathPrefix:            "",
			streamGroupingEnabled: true,
			streamGroupingLevel:   2,
			fileKey:               "users/2024-01-01/data.csv",
			expectedStreamName:    "users/2024-01-01",
		},
		{
			name:                  "with path prefix - remove prefix",
			pathPrefix:            "data/raw",
			streamGroupingEnabled: true,
			streamGroupingLevel:   1,
			fileKey:               "data/raw/users/2024-01-01/data.csv",
			expectedStreamName:    "users",
		},
		{
			name:                  "file at root level",
			pathPrefix:            "",
			streamGroupingEnabled: true,
			streamGroupingLevel:   1,
			fileKey:               "data.csv",
			expectedStreamName:    "data.csv",
		},
		{
			name:                  "single folder level",
			pathPrefix:            "",
			streamGroupingEnabled: true,
			streamGroupingLevel:   1,
			fileKey:               "users/data.csv",
			expectedStreamName:    "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3{
				config: &Config{
					PathPrefix:            tt.pathPrefix,
					StreamGroupingEnabled: tt.streamGroupingEnabled,
					StreamGroupingLevel:   tt.streamGroupingLevel,
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
				assert.True(t, tt.config.StreamGroupingEnabled)
				assert.Equal(t, 1, tt.config.StreamGroupingLevel)
				assert.NotZero(t, tt.config.MaxThreads)
				assert.NotZero(t, tt.config.BatchSize)
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


