package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQueryEngines(t *testing.T) {
	testCases := []struct {
		name     string
		values   []string
		expected []QueryEngine
		wantErr  bool
	}{
		{name: "empty input", values: nil, expected: []QueryEngine{}},
		{name: "normalizes case and spacing", values: []string{" Spark ", "DuckDB"}, expected: []QueryEngine{QueryEngineSpark, QueryEngineDuckDB}},
		{name: "drops blanks", values: []string{"spark", "", "  "}, expected: []QueryEngine{QueryEngineSpark}},
		{name: "deduplicates", values: []string{"hive", "hive"}, expected: []QueryEngine{QueryEngineHive}},
		// A silently ignored typo would widen the available set, so it must be an error.
		{name: "rejects unknown engine", values: []string{"spark", "sparkk"}, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			engines, err := ParseQueryEngines(tc.values)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, engines)
		})
	}
}

// The engine names spec hands a client are the same names discover accepts back, so a
// client can round-trip the list without translating it.
func TestQueryEngineCatalogRoundTripsThroughParse(t *testing.T) {
	served, ok := QueryEngineCatalog()["engines"].([]QueryEngineSpec)
	require.True(t, ok)
	require.NotEmpty(t, served)

	names := make([]string, 0, len(served))
	for _, spec := range served {
		names = append(names, string(spec.Engine))
	}

	parsed, err := ParseQueryEngines(names)
	require.NoError(t, err)
	assert.Len(t, parsed, len(served))
}

func TestAvailableUpdateTypes(t *testing.T) {
	testCases := []struct {
		name     string
		engines  []QueryEngine
		expected []UpdateType
	}{
		{
			// No engines means unconstrained: everything OLake can write stays on the table.
			name:     "no engines leaves every writable format",
			engines:  nil,
			expected: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector},
		},
		{
			name:     "single engine reading every format",
			engines:  []QueryEngine{QueryEngineSpark},
			expected: []UpdateType{UpdateTypeEquality, UpdateTypePosition, UpdateTypeDeletionVector},
		},
		{
			// Dremio reads only positional deletes, so the intersection drops to positional.
			name:     "one restrictive engine narrows the set",
			engines:  []QueryEngine{QueryEngineSpark, QueryEngineDuckDB, QueryEngineDremio},
			expected: []UpdateType{UpdateTypePosition},
		},
		{
			// Snowflake cannot read equality deletes.
			name:     "restrictive engine alone",
			engines:  []QueryEngine{QueryEngineSnowflake},
			expected: []UpdateType{UpdateTypePosition, UpdateTypeDeletionVector},
		},
		{
			// Athena only reads v2 tables, so deletion vectors drop out.
			name:     "engine without deletion vector support drops dv",
			engines:  []QueryEngine{QueryEngineSpark, QueryEngineAthena},
			expected: []UpdateType{UpdateTypeEquality, UpdateTypePosition},
		},
		{
			// Databricks applies no v2 delete files, so deletion vectors are all that remain.
			name:     "deletion vector only engine",
			engines:  []QueryEngine{QueryEngineSpark, QueryEngineDatabricks},
			expected: []UpdateType{UpdateTypeDeletionVector},
		},
		{
			// Databricks reads only deletion vectors, Athena none of them.
			name:     "disjoint engines leave nothing",
			engines:  []QueryEngine{QueryEngineDatabricks, QueryEngineAthena},
			expected: []UpdateType{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, AvailableUpdateTypes(tc.engines))
		})
	}
}

func TestPreferredUpdateType(t *testing.T) {
	// Equality outranks positional: it needs no identifier -> RowLocation index.
	assert.Equal(t, UpdateTypeEquality, PreferredUpdateType([]UpdateType{UpdateTypeEquality, UpdateTypePosition}))
	assert.Equal(t, UpdateTypePosition, PreferredUpdateType([]UpdateType{UpdateTypePosition}))
	// Deletion vectors need a v3 table, so positional stays the default while it is readable.
	assert.Equal(t, UpdateTypePosition, PreferredUpdateType([]UpdateType{UpdateTypePosition, UpdateTypeDeletionVector}))
	assert.Equal(t, UpdateType(""), PreferredUpdateType(nil))
}

func TestUpdateTypeValidateAgainst(t *testing.T) {
	testCases := []struct {
		name       string
		updateType UpdateType
		available  []UpdateType
		wantErr    bool
	}{
		// Discover ran without target engines, so nothing narrows the choice.
		{name: "equality with no available list", updateType: UpdateTypeEquality},
		{name: "equality is offered", updateType: UpdateTypeEquality, available: []UpdateType{UpdateTypeEquality, UpdateTypePosition}},
		// Hand-edited streams file, or engines that changed without a re-discover.
		{name: "equality is not offered", updateType: UpdateTypeEquality, available: []UpdateType{UpdateTypePosition}, wantErr: true},
		{name: "positional is offered", updateType: UpdateTypePosition, available: []UpdateType{UpdateTypePosition}},
		{name: "deletion vector is offered", updateType: UpdateTypeDeletionVector, available: []UpdateType{UpdateTypePosition, UpdateTypeDeletionVector}},
		{name: "deletion vector is not offered", updateType: UpdateTypeDeletionVector, available: []UpdateType{UpdateTypeEquality, UpdateTypePosition}, wantErr: true},
		{name: "garbage value", updateType: UpdateType("nope"), wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.updateType.ValidateAgainst(tc.available)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestGetWrappedCatalogUsesEngineDerivedUpdateType(t *testing.T) {
	streams := []*Stream{{Name: "users", Namespace: "public", Schema: NewTypeSchema()}}

	// Snowflake cannot read equality deletes, so the seeded default drops to positional.
	catalog := GetWrappedCatalog(streams, "postgres", []QueryEngine{QueryEngineSpark, QueryEngineSnowflake})

	assert.Equal(t, string(UpdateTypePosition), catalog.SelectedStreams["public"][0].UpdateType)
}

// The engines themselves are an input, never a field: only the list they produce is written.
func TestLogCatalogPersistsOnlyTheDerivedList(t *testing.T) {
	streams := []*Stream{{
		Name: "users", Namespace: "public", Schema: NewTypeSchema(),
		DefaultStreamProperties: &DefaultStreamProperties{},
	}}

	catalog := GetWrappedCatalog(streams, "postgres", []QueryEngine{QueryEngineSpark, QueryEngineDuckDB})
	serialized, err := json.Marshal(catalog)
	require.NoError(t, err)
	assert.NotContains(t, string(serialized), "target_query_engines")
}

func TestMergeUpdateType(t *testing.T) {
	testCases := []struct {
		name     string
		existing string
		engines  []QueryEngine
		expected string
	}{
		{
			// Legacy blank always meant equality; recording it keeps blank for "needs a choice".
			name: "no engines records a legacy blank as equality", existing: "", engines: nil, expected: "eq",
		},
		{
			name: "no engines leaves a set value alone", existing: "dv", engines: nil, expected: "dv",
		},
		{
			name: "readable value is kept", existing: "pos",
			engines: []QueryEngine{QueryEngineSpark, QueryEngineSnowflake}, expected: "pos",
		},
		{
			// The stream was configured before Snowflake joined the target engines.
			name: "unreadable value is cleared", existing: "eq",
			engines: []QueryEngine{QueryEngineSpark, QueryEngineSnowflake}, expected: "",
		},
		{
			name: "legacy blank readable as equality", existing: "",
			engines: []QueryEngine{QueryEngineSpark}, expected: "eq",
		},
		{
			// Blank meant equality, which Snowflake cannot read.
			name: "legacy blank unreadable as equality is cleared", existing: "",
			engines: []QueryEngine{QueryEngineSnowflake}, expected: "",
		},
		{
			name: "readable deletion vector is kept", existing: "dv",
			engines: []QueryEngine{QueryEngineSpark, QueryEngineTrino}, expected: "dv",
		},
		{
			// Athena reads equality, but the switch must be the user's choice, not a silent fallback.
			name: "unreadable deletion vector is cleared", existing: "dv",
			engines: []QueryEngine{QueryEngineSpark, QueryEngineAthena}, expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &StreamMetadata{StreamName: "users", UpdateType: tc.existing}
			mergeUpdateType(metadata, "public.users", tc.engines)
			assert.Equal(t, tc.expected, metadata.UpdateType)
		})
	}
}
