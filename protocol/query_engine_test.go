package protocol

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveAvailableUpdateTypes(t *testing.T) {
	writeDestination := func(t *testing.T, content string) string {
		path := filepath.Join(t.TempDir(), "destination.json")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}

	testCases := []struct {
		name        string
		flag        []string
		destination string // file content; empty means --destination not passed
		expected    []types.UpdateType
		wantErr     bool
	}{
		{
			// Nothing is persisted, so omitted flags mean unconstrained, not "reuse".
			name:     "no flags leave every format",
			expected: []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition, types.UpdateTypeDeletionVector},
		},
		{
			// Flag values are normalized; Athena cannot read deletion vectors.
			name:     "engine flag narrows",
			flag:     []string{" Spark ", "athena"},
			expected: []types.UpdateType{types.UpdateTypeEquality, types.UpdateTypePosition},
		},
		{
			name:    "unknown engine fails the run",
			flag:    []string{"spark", "sparkk"},
			wantErr: true,
		},
		{
			// Databricks reads only deletion vectors and Athena none, so no format fits both.
			// Append needs no delete format, so discover still succeeds.
			name:     "engines without a common delete format still discover",
			flag:     []string{"databricks", "athena"},
			expected: []types.UpdateType{},
		},
		{
			name:        "destination catalog is read from --destination",
			destination: `{"type":"ICEBERG","writer":{"catalog_type":"unity"}}`,
			expected:    []types.UpdateType{types.UpdateTypeDeletionVector},
		},
		{
			// Parquet writes no delete files, so streams can only append.
			name:        "parquet destination offers none",
			flag:        []string{"athena"},
			destination: `{"type":"PARQUET","writer":{}}`,
			expected:    []types.UpdateType{},
		},
		{
			name:        "malformed destination fails the run",
			destination: `{"type":"ICEBERG","writer":`,
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destinationPath := "not-set"
			if tc.destination != "" {
				destinationPath = writeDestination(t, tc.destination)
			}
			targetQueryEngines, destinationConfigPath, destinationConfig = tc.flag, destinationPath, nil
			t.Cleanup(func() {
				targetQueryEngines, destinationConfigPath, destinationConfig = nil, "not-set", nil
			})

			// PreRunE loads --destination before RunE resolves, as discover does.
			var available []types.UpdateType
			err := loadDestinationConfig()
			if err == nil {
				available, err = resolveAvailableUpdateTypes()
			}
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, available)
		})
	}
}
