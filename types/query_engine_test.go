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
			assert.Equal(t, tc.expected, availableUpdateTypes(tc.engines, ""))
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
	streams := []*Stream{{Name: "users", Namespace: "public", Schema: NewTypeSchema(), DefaultStreamProperties: &DefaultStreamProperties{}}}

	// Snowflake cannot read equality deletes, so the seeded default drops to positional.
	available := availableUpdateTypes([]QueryEngine{QueryEngineSpark, QueryEngineSnowflake}, "")
	ApplyDestinationDefaults(streams, available, AvailableWriteModes(Iceberg, nil))
	catalog := GetWrappedCatalog(streams)

	assert.Equal(t, UpdateTypePosition, catalog.Streams[0].GetUpdateType())
}

func TestGetWrappedCatalogKafkaPositionalYieldsWhenUnreadable(t *testing.T) {
	kafkaSupported := []UpdateType{UpdateTypePosition, UpdateTypeDeletionVector}
	kafkaStream := func(engines []QueryEngine) *Stream {
		streams := []*Stream{{
			Name: "users", Namespace: "public", Schema: NewTypeSchema(),
			DefaultStreamProperties: &DefaultStreamProperties{},
		}}
		available, err := AvailableUpdateTypes(engines, &WriterConfig{Type: Iceberg, WriterConfig: map[string]any{}}, kafkaSupported)
		require.NoError(t, err)
		ApplyDestinationDefaults(streams, available, AvailableWriteModes(Iceberg, nil))
		return GetWrappedCatalog(streams).Streams[0].Stream
	}

	// Spark reads pos, the first format Kafka allows.
	spark := kafkaStream([]QueryEngine{QueryEngineSpark})
	assert.Equal(t, UpdateTypePosition, spark.DefaultStreamProperties.UpdateType)

	// Databricks reads only deletion vectors, so the default cannot be positional.
	databricks := kafkaStream([]QueryEngine{QueryEngineDatabricks})
	assert.Equal(t, UpdateTypeDeletionVector, databricks.DefaultStreamProperties.UpdateType)
	upsert := &ConfiguredStream{
		Stream:         databricks,
		StreamMetadata: StreamMetadata{AppendMode: new(false), DedupKeys: []string{"_kafka_key"}},
	}
	assert.Equal(t, UpdateTypeDeletionVector, upsert.GetUpdateType())
}

// The engines themselves are an input, never a field: only the list they produce is written.
func TestLogCatalogPersistsOnlyTheDerivedList(t *testing.T) {
	streams := []*Stream{{
		Name: "users", Namespace: "public", Schema: NewTypeSchema(),
		DefaultStreamProperties: &DefaultStreamProperties{},
	}}

	available := availableUpdateTypes([]QueryEngine{QueryEngineSpark, QueryEngineDuckDB}, "")
	ApplyDestinationDefaults(streams, available, AvailableWriteModes(Iceberg, nil))
	catalog := GetWrappedCatalog(streams)
	serialized, err := json.Marshal(catalog)
	require.NoError(t, err)
	assert.NotContains(t, string(serialized), "target_query_engines")
}

// An omitted update_type follows default_stream_properties, which a new discover derives from the
// current engines; merging keeps the previous format instead of letting it switch silently.
func TestMergeCatalogsKeepsImplicitUpdateType(t *testing.T) {
	stream := func(defaultType UpdateType) *Stream {
		return &Stream{
			Name: "users", Namespace: "public", Schema: NewTypeSchema(),
			DefaultStreamProperties: &DefaultStreamProperties{UpdateType: defaultType},
		}
	}
	testCases := []struct {
		name            string
		previousDefault UpdateType
		currentDefault  UpdateType
		engines         []QueryEngine
		expected        *string
	}{
		{
			name: "unchanged default stays omitted", previousDefault: UpdateTypeEquality, currentDefault: UpdateTypeEquality,
			engines: []QueryEngine{QueryEngineSpark}, expected: nil,
		},
		{
			name: "changed default keeps the readable previous format", previousDefault: UpdateTypePosition, currentDefault: UpdateTypeEquality,
			engines: []QueryEngine{QueryEngineSpark}, expected: new("pos"),
		},
		{
			// Databricks reads deletion vectors only, so the pinned equality moves to them.
			name: "changed default with an unreadable previous equality moves to the default", previousDefault: UpdateTypeEquality, currentDefault: UpdateTypeDeletionVector,
			engines: []QueryEngine{QueryEngineDatabricks}, expected: new("dv"),
		},
		{
			// Only equality moves on its own; any other switch must be the user's choice.
			name: "changed default with an unreadable previous format is cleared", previousDefault: UpdateTypePosition, currentDefault: UpdateTypeDeletionVector,
			engines: []QueryEngine{QueryEngineDatabricks}, expected: new(""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			oldCatalog := &Catalog{
				Streams:         []*ConfiguredStream{{Stream: stream(tc.previousDefault)}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			}
			newCatalog := &Catalog{
				Streams:         []*ConfiguredStream{{Stream: stream(tc.currentDefault)}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			}

			merged := mergeCatalogs(oldCatalog, newCatalog, availableUpdateTypes(tc.engines, ""))
			assert.Equal(t, tc.expected, merged.SelectedStreams["public"][0].UpdateType)
		})
	}
}

func TestMergeUpdateType(t *testing.T) {
	testCases := []struct {
		name     string
		existing *string
		engines  []QueryEngine
		expected *string
	}{
		{
			// Omitted follows the stream default, which discover already derives from the engines.
			name: "omitted stays omitted", existing: nil,
			engines: []QueryEngine{QueryEngineSnowflake}, expected: nil,
		},
		{
			// Legacy blank always meant equality; recording it keeps blank for "needs a choice".
			name: "no engines records a legacy blank as equality", existing: new(""), engines: nil, expected: new("eq"),
		},
		{
			name: "no engines leaves a set value alone", existing: new("dv"), engines: nil, expected: new("dv"),
		},
		{
			name: "readable value is kept", existing: new("pos"),
			engines: []QueryEngine{QueryEngineSpark, QueryEngineSnowflake}, expected: new("pos"),
		},
		{
			// The stream was configured before Snowflake joined the target engines; the writer
			// migrates equality deletes to positional ones, so no user choice is needed.
			name: "unreadable equality moves to positional", existing: new("eq"),
			engines: []QueryEngine{QueryEngineSpark, QueryEngineSnowflake}, expected: new("pos"),
		},
		{
			name: "legacy blank readable as equality", existing: new(""),
			engines: []QueryEngine{QueryEngineSpark}, expected: new("eq"),
		},
		{
			// Blank meant equality, which Snowflake cannot read.
			name: "legacy blank unreadable as equality moves to positional", existing: new(""),
			engines: []QueryEngine{QueryEngineSnowflake}, expected: new("pos"),
		},
		{
			// Databricks reads neither equality nor positional deletes.
			name: "unreadable equality without positional moves to deletion vectors", existing: new("eq"),
			engines: []QueryEngine{QueryEngineDatabricks}, expected: new("dv"),
		},
		{
			name: "readable deletion vector is kept", existing: new("dv"),
			engines: []QueryEngine{QueryEngineSpark, QueryEngineTrino}, expected: new("dv"),
		},
		{
			// Athena reads equality, but the switch must be the user's choice, not a silent fallback.
			name: "unreadable deletion vector is cleared", existing: new("dv"),
			engines: []QueryEngine{QueryEngineSpark, QueryEngineAthena}, expected: new(""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &StreamMetadata{StreamName: "users", UpdateType: tc.existing}
			mergeUpdateType(metadata, "public.users", availableUpdateTypes(tc.engines, ""))
			assert.Equal(t, tc.expected, metadata.UpdateType)
		})
	}
}
