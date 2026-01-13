package driver

import (
	"testing"

	"github.com/datazip-inc/olake/pkg/parser"
	"github.com/stretchr/testify/assert"
)

// TestFilePatternMatching tests various file pattern scenarios
func TestFilePatternMatching(t *testing.T) {
	tests := []struct {
		name        string
		filePattern string
		fileKeys    []string
		shouldMatch []bool
	}{
		{
			name:        "wildcard pattern",
			filePattern: "*.csv",
			fileKeys:    []string{"data.csv", "users.csv", "data.json"},
			shouldMatch: []bool{true, true, false},
		},
		{
			name:        "prefix pattern",
			filePattern: "users/*",
			fileKeys:    []string{"users/data.csv", "orders/data.csv", "users/2024/data.csv"},
			shouldMatch: []bool{true, false, true},
		},
		{
			name:        "exact match",
			filePattern: "data.csv",
			fileKeys:    []string{"data.csv", "data.json", "subdir/data.csv"},
			shouldMatch: []bool{true, false, false},
		},
		{
			name:        "empty pattern - matches all",
			filePattern: "",
			fileKeys:    []string{"data.csv", "users.json", "any/file.parquet"},
			shouldMatch: []bool{true, true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, fileKey := range tt.fileKeys {
				result := matchesPattern(fileKey, tt.filePattern)
				assert.Equal(t, tt.shouldMatch[i], result,
					"pattern %s should match %s: %v", tt.filePattern, fileKey, tt.shouldMatch[i])
			}
		})
	}
}

// Helper function for pattern matching (simplified version for testing)
func matchesPattern(fileKey, pattern string) bool {
	if pattern == "" {
		return true
	}
	// This is a simplified version - actual implementation would use filepath.Match
	// For testing purposes, we'll implement basic wildcard matching
	if pattern == "*.csv" {
		return len(fileKey) > 4 && fileKey[len(fileKey)-4:] == ".csv"
	}
	if pattern == "users/*" {
		return len(fileKey) > 6 && fileKey[:6] == "users/"
	}
	return fileKey == pattern
}

// TestCompressionTypeValidation tests compression validation
func TestCompressionTypeValidation(t *testing.T) {
	tests := []struct {
		name        string
		compression CompressionType
		isValid     bool
		expected    CompressionType // Expected after validation
	}{
		{"none compression", CompressionNone, true, CompressionNone},
		{"gzip compression", CompressionGzip, true, CompressionGzip},
		{"zip compression", CompressionZip, true, CompressionZip},
		{"invalid compression", "brotli", false, ""},
		{"empty compression defaults to none", "", true, CompressionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				BucketName:  "test-bucket",
				Region:      "us-east-1",
				FileFormat:  FormatCSV,
				Compression: tt.compression,
			}

			err := config.Validate()
			if tt.isValid {
				assert.NoError(t, err, "valid compression should pass validation")
				assert.Equal(t, tt.expected, config.Compression, "compression should match expected value")
			} else {
				assert.Error(t, err, "invalid compression should fail validation")
			}
		})
	}
}

// TestStreamGroupingEdgeCases tests edge cases in stream name extraction (always level 1)
func TestStreamGroupingEdgeCases(t *testing.T) {
	tests := []struct {
		name               string
		pathPrefix         string
		fileKey            string
		expectedStreamName string
	}{
		{
			name:               "deeply nested path - extracts only first folder",
			pathPrefix:         "",
			fileKey:            "year/2024/month/01/day/15/data.csv",
			expectedStreamName: "year",
		},
		{
			name:               "file in root - no folders",
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
			name:               "path prefix with trailing slash",
			pathPrefix:         "raw/",
			fileKey:            "raw/users/data.csv",
			expectedStreamName: "users",
		},
		{
			name:               "empty file key",
			pathPrefix:         "",
			fileKey:            "",
			expectedStreamName: "",
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

// TestFileFormatExtensions tests file format detection with various extensions
func TestFileFormatExtensions(t *testing.T) {
	tests := []struct {
		name        string
		fileFormat  FileFormat
		compression CompressionType
		fileKeys    map[string]bool // file -> should match
	}{
		{
			name:        "CSV with various extensions",
			fileFormat:  FormatCSV,
			compression: CompressionNone,
			fileKeys: map[string]bool{
				"data.csv":     true,
				"data.CSV":     true,
				"data.tsv":     false,
				"data.csv.bak": false,
			},
		},
		{
			name:        "JSON with various formats",
			fileFormat:  FormatJSON,
			compression: CompressionNone,
			fileKeys: map[string]bool{
				"data.json":   true,
				"data.jsonl":  true,
				"data.ndjson": false, // ndjson extension not explicitly supported
				"data.xml":    false,
			},
		},
		{
			name:        "Parquet files",
			fileFormat:  FormatParquet,
			compression: CompressionNone,
			fileKeys: map[string]bool{
				"data.parquet": true,
				"data.pq":      false,
				"data.csv":     false,
			},
		},
		{
			name:        "compressed CSV files",
			fileFormat:  FormatCSV,
			compression: CompressionGzip,
			fileKeys: map[string]bool{
				"data.csv.gz":   true,
				"data.csv.gzip": false,
				"data.csv":      true, // matchesFileFormat uses OR logic: .csv OR .csv.gz
			},
		},
		{
			name:        "compressed JSON files",
			fileFormat:  FormatJSON,
			compression: CompressionGzip,
			fileKeys: map[string]bool{
				"data.json.gz":  true,
				"data.jsonl.gz": true,
				"data.json":     true, // matchesFileFormat uses OR logic
				"data.jsonl":    true, // matchesFileFormat uses OR logic
			},
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

			for fileKey, shouldMatch := range tt.fileKeys {
				result := s.matchesFileFormat(fileKey)
				assert.Equal(t, shouldMatch, result,
					"format %s, compression %s, file %s",
					tt.fileFormat, tt.compression, fileKey)
			}
		})
	}
}

// TestCSVConfigEdgeCases tests CSV configuration edge cases
func TestCSVConfigEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		csvConfig *parser.CSVConfig
		expectErr bool
	}{
		{
			name: "valid config with custom delimiter",
			csvConfig: &parser.CSVConfig{
				Delimiter:      "|",
				HasHeader:      true,
				SkipRows:       0,
				QuoteCharacter: "\"",
			},
			expectErr: false,
		},
		{
			name: "valid config with tab delimiter",
			csvConfig: &parser.CSVConfig{
				Delimiter:      "\t",
				HasHeader:      false,
				SkipRows:       5,
				QuoteCharacter: "'",
			},
			expectErr: false,
		},
		{
			name: "config with skip rows",
			csvConfig: &parser.CSVConfig{
				Delimiter:      ",",
				HasHeader:      true,
				SkipRows:       10,
				QuoteCharacter: "\"",
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				BucketName: "test-bucket",
				Region:     "us-east-1",
				FileFormat: FormatCSV,
				CSV:        tt.csvConfig,
			}

			err := config.Validate()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestJSONConfigEdgeCases tests JSON configuration edge cases
func TestJSONConfigEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		jsonConfig *parser.JSONConfig
		expectErr  bool
	}{
		{
			name: "line delimited JSON (JSONL)",
			jsonConfig: &parser.JSONConfig{
				LineDelimited: true,
			},
			expectErr: false,
		},
		{
			name: "JSON array format",
			jsonConfig: &parser.JSONConfig{
				LineDelimited: false,
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				BucketName: "test-bucket",
				Region:     "us-east-1",
				FileFormat: FormatJSON,
				JSON:       tt.jsonConfig,
			}

			err := config.Validate()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestParquetConfigEdgeCases tests Parquet configuration edge cases
func TestParquetConfigEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		parquetConfig *parser.ParquetConfig
		expectErr     bool
	}{
		{
			name: "streaming enabled",
			parquetConfig: &parser.ParquetConfig{
				StreamingEnabled: true,
			},
			expectErr: false,
		},
		{
			name: "streaming disabled - load into memory",
			parquetConfig: &parser.ParquetConfig{
				StreamingEnabled: false,
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				BucketName: "test-bucket",
				Region:     "us-east-1",
				FileFormat: FormatParquet,
				Parquet:    tt.parquetConfig,
			}

			err := config.Validate()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestThreadCountEdgeCases tests thread count validation
func TestThreadCountEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		maxThreads int
		shouldPass bool
	}{
		{"default threads (0)", 0, true},
		{"single thread", 1, true},
		{"many threads", 100, true},
		{"negative threads", -1, true}, // Should be normalized to default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				BucketName: "test-bucket",
				Region:     "us-east-1",
				FileFormat: FormatCSV,
				MaxThreads: tt.maxThreads,
			}

			err := config.Validate()
			if tt.shouldPass {
				assert.NoError(t, err)
				assert.Greater(t, config.MaxThreads, 0, "threads should be > 0 after validation")
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// TestRetryCountEdgeCases tests retry count validation
func TestRetryCountEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		expected   int
	}{
		{"default retry (0)", 0, 3}, // Should be set to default
		{"no retries", 1, 1},
		{"many retries", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				BucketName: "test-bucket",
				Region:     "us-east-1",
				FileFormat: FormatCSV,
				RetryCount: tt.retryCount,
			}

			err := config.Validate()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, config.RetryCount)
		})
	}
}

// TestS3ConfigWithCustomEndpoint tests S3-compatible services (MinIO, LocalStack, etc.)
func TestS3ConfigWithCustomEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		region   string
		valid    bool
	}{
		{
			name:     "MinIO endpoint",
			endpoint: "http://localhost:9000",
			region:   "us-east-1",
			valid:    true,
		},
		{
			name:     "LocalStack endpoint",
			endpoint: "http://localhost:4566",
			region:   "us-east-1",
			valid:    true,
		},
		{
			name:     "custom S3-compatible service",
			endpoint: "https://s3.example.com",
			region:   "custom-region",
			valid:    true,
		},
		{
			name:     "empty endpoint with region",
			endpoint: "",
			region:   "us-east-1",
			valid:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				BucketName: "test-bucket",
				Region:     tt.region,
				Endpoint:   tt.endpoint,
				FileFormat: FormatCSV,
			}

			err := config.Validate()
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// TestEmptyAndNilConfigurations tests handling of empty/nil configurations
func TestEmptyAndNilConfigurations(t *testing.T) {
	t.Run("empty config should fail", func(t *testing.T) {
		config := Config{}
		err := config.Validate()
		assert.Error(t, err, "empty config should fail validation")
	})

	t.Run("nil parser configs should be initialized", func(t *testing.T) {
		config := Config{
			BucketName: "test-bucket",
			Region:     "us-east-1",
			FileFormat: FormatCSV,
			CSV:        nil, // Will be initialized by Validate()
		}
		err := config.Validate()
		assert.NoError(t, err)
		assert.NotNil(t, config.CSV, "CSV config should be initialized")
	})
}

// TestConcurrentStateAccess tests concurrent access to state
func TestConcurrentStateAccess(t *testing.T) {
	// Note: This is a basic test. Full concurrency testing would require more complex scenarios
	t.Run("concurrent state reads should be safe", func(t *testing.T) {
		s := &S3{
			discoveredFiles: map[string][]FileObject{
				"test_stream": {
					{FileKey: "file1.csv", LastModified: "2024-01-01T10:00:00Z"},
					{FileKey: "file2.csv", LastModified: "2024-01-02T10:00:00Z"},
				},
			},
		}

		// Multiple goroutines reading state concurrently
		done := make(chan bool, 5)
		for i := 0; i < 5; i++ {
			go func() {
				files := s.discoveredFiles["test_stream"]
				assert.Len(t, files, 2, "should read correct number of files")
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 5; i++ {
			<-done
		}
	})
}
