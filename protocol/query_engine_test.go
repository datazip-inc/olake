package protocol

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveUpdateTypeConstraints(t *testing.T) {
	writeDestination := func(t *testing.T, content string) string {
		path := filepath.Join(t.TempDir(), "destination.json")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}

	testCases := []struct {
		name        string
		flag        []string
		destination string // file content; empty means --destination not passed
		expected    types.UpdateTypeConstraints
		wantErr     bool
	}{
		{
			// Nothing is persisted, so omitted flags mean unconstrained, not "reuse".
			name:     "no flags leave everything unconstrained",
			expected: types.UpdateTypeConstraints{Engines: []types.QueryEngine{}},
		},
		{
			name: "engine flag is normalized",
			flag: []string{" Spark ", "duckdb"},
			expected: types.UpdateTypeConstraints{
				Engines: []types.QueryEngine{types.QueryEngineSpark, types.QueryEngineDuckDB},
			},
		},
		{
			name:    "unknown engine fails the run",
			flag:    []string{"spark", "sparkk"},
			wantErr: true,
		},
		{
			// Databricks reads only deletion vectors and Athena none, so no format fits both.
			name:    "engines without a common delete format fail the run",
			flag:    []string{"databricks", "athena"},
			wantErr: true,
		},
		{
			name:        "destination catalog is read from --destination",
			destination: `{"type":"ICEBERG","writer":{"catalog_type":"unity"}}`,
			expected: types.UpdateTypeConstraints{
				Engines: []types.QueryEngine{},
				Catalog: "unity",
			},
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
			targetQueryEngines, destinationConfigPath = tc.flag, destinationPath
			destinationConfig, updateTypeConstraints = nil, types.UpdateTypeConstraints{}
			t.Cleanup(func() {
				targetQueryEngines, destinationConfigPath = nil, "not-set"
				destinationConfig, updateTypeConstraints = nil, types.UpdateTypeConstraints{}
			})

			err := resolveUpdateTypeConstraints()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, updateTypeConstraints)
		})
	}
}
