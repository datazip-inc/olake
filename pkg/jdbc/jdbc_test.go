package jdbc

import (
	"encoding/json"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLFilterBinary tests that a binary filter value is hex from state version 8 and text on older state. The
// schema is read from catalog JSON under that state version, as a sync reads its catalog.
func TestSQLFilterBinary(t *testing.T) {
	tests := []struct {
		name         string
		stateVersion int
		value        string
		expected     string
	}{
		{name: "hex", stateVersion: 8, value: "616263", expected: "`data` = X'616263'"},
		{name: "text before state version 8", stateVersion: 7, value: "616263", expected: "`data` = '616263'"},
		{name: "non-hex text before state version 8", stateVersion: 7, value: "update12", expected: "`data` = 'update12'"},
	}

	old := constants.LoadedStateVersion
	t.Cleanup(func() { constants.LoadedStateVersion = old })

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			constants.LoadedStateVersion = tc.stateVersion
			schema := types.NewTypeSchema()
			require.NoError(t, json.Unmarshal([]byte(`{"properties": {"data": {"type": ["binary"]}}}`), schema))
			stream := &types.ConfiguredStream{
				Stream: &types.Stream{Name: "users", Namespace: "db", Schema: schema},
				StreamMetadata: types.StreamMetadata{
					Normalization: true,
					FilterConfig: &types.FilterConfig{
						Conditions: []types.FilterCondition{{Column: "data", Operator: "=", Value: tc.value}},
					},
				},
			}

			filter, err := SQLFilter(stream, "mysql", "")
			require.NoError(t, err)
			assert.Equal(t, tc.expected, filter)
		})
	}
}
