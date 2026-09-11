package protocol

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveTargetQueryEngines(t *testing.T) {
	testCases := []struct {
		name     string
		flag     []string
		expected []types.QueryEngine
		wantErr  bool
	}{
		{
			// Nothing is persisted, so an omitted flag means unconstrained, not "reuse".
			name:     "no flag leaves the selection empty",
			expected: []types.QueryEngine{},
		},
		{
			name:     "flag is normalized",
			flag:     []string{" Spark ", "duckdb"},
			expected: []types.QueryEngine{types.QueryEngineSpark, types.QueryEngineDuckDB},
		},
		{
			name:    "unknown engine fails the run",
			flag:    []string{"spark", "sparkk"},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			targetQueryEngines, queryEngines = tc.flag, nil
			t.Cleanup(func() { targetQueryEngines, queryEngines = nil, nil })

			err := resolveTargetQueryEngines()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, queryEngines)
		})
	}
}
