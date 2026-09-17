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
			expected: []UpdateType{UpdateTypeEquality, UpdateTypePosition},
		},
		{
			name:     "single engine reading both formats",
			engines:  []QueryEngine{QueryEngineSpark},
			expected: []UpdateType{UpdateTypeEquality, UpdateTypePosition},
		},
		{
			// DuckDB cannot resolve equality deletes, so the intersection drops to positional.
			name:     "one restrictive engine narrows the set",
			engines:  []QueryEngine{QueryEngineSpark, QueryEngineDuckDB, QueryEngineHive},
			expected: []UpdateType{UpdateTypePosition},
		},
		{
			name:     "restrictive engine alone",
			engines:  []QueryEngine{QueryEngineSnowflake},
			expected: []UpdateType{UpdateTypePosition},
		},
		{
			// Spark and Trino both read deletion vectors, but OLake cannot write them yet.
			name:     "unwritable formats never surface",
			engines:  []QueryEngine{QueryEngineSpark, QueryEngineTrino},
			expected: []UpdateType{UpdateTypeEquality, UpdateTypePosition},
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
		{name: "deletion vector is not writable", updateType: UpdateTypeDeletionVector, wantErr: true},
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

	// DuckDB cannot read equality deletes, so the seeded default drops to positional.
	catalog := GetWrappedCatalog(streams, "postgres", []QueryEngine{QueryEngineSpark, QueryEngineDuckDB})

	assert.Equal(t, string(UpdateTypePosition), catalog.SelectedStreams["public"][0].UpdateType)
}

// The engines themselves are an input, never a field: only the list they produce is written.
func TestLogCatalogPersistsOnlyTheDerivedList(t *testing.T) {
	streams := []*Stream{{
		Name: "users", Namespace: "public", Schema: NewTypeSchema(),
		DefaultStreamProperties: &DefaultStreamProperties{},
	}}

	catalog := GetWrappedCatalog(streams, "postgres", []QueryEngine{QueryEngineSpark, QueryEngineDuckDB})
	serialised, err := json.Marshal(catalog)
	require.NoError(t, err)
	assert.NotContains(t, string(serialised), "target_query_engines")
}

func TestMergeUpdateType(t *testing.T) {
	testCases := []struct {
		name     string
		existing string
		engines  []QueryEngine
		expected string
	}{
		{
			// Legacy catalogs carry no engines; their recorded value must survive untouched.
			name: "no engines leaves the value alone", existing: "", engines: nil, expected: "",
		},
		{
			name: "readable value is kept", existing: "pos",
			engines: []QueryEngine{QueryEngineSpark, QueryEngineDuckDB}, expected: "pos",
		},
		{
			// The stream was configured before DuckDB joined the target engines.
			name: "unreadable value is re-picked", existing: "eq",
			engines: []QueryEngine{QueryEngineSpark, QueryEngineDuckDB}, expected: "pos",
		},
		{
			name: "blank value is filled", existing: "",
			engines: []QueryEngine{QueryEngineSpark}, expected: "eq",
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
