package types

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func icebergConfig(writer map[string]any) *WriterConfig {
	return &WriterConfig{Type: Iceberg, WriterConfig: writer}
}

func TestCatalogTypeFromConfig(t *testing.T) {
	testCases := []struct {
		name     string
		config   *WriterConfig
		expected string
		wantErr  bool
	}{
		// Discover run without --destination.
		{name: "no destination", config: nil, expected: ""},
		// Parquet writes no delete files, so delete formats do not apply.
		{name: "non-iceberg destination", config: &WriterConfig{Type: Parquet, WriterConfig: map[string]any{}}, expected: ""},
		{name: "explicit catalog", config: icebergConfig(map[string]any{"catalog_type": "unity"}), expected: "unity"},
		{name: "missing catalog defaults to glue", config: icebergConfig(map[string]any{}), expected: "glue"},
		// Read before iceberg's validation, which would report this as plain "rest".
		{name: "rest-family catalog keeps its name", config: icebergConfig(map[string]any{"catalog_type": "s3tables"}), expected: "s3tables"},
		// Left unconstrained, a typo would offer formats the catalog cannot apply.
		{name: "unknown catalog fails", config: icebergConfig(map[string]any{"catalog_type": "glu"}), wantErr: true},
		{name: "unreadable writer config fails", config: &WriterConfig{Type: Iceberg, WriterConfig: "not an object"}, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CatalogTypeFromConfig(tc.config)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

// A catalog the destination accepts but the matrix lacks would fail every discover against it.
func TestIcebergCatalogsCoverDestinationSpec(t *testing.T) {
	raw, err := os.ReadFile("../destination/iceberg/resources/spec.json")
	require.NoError(t, err)

	var spec struct {
		Properties struct {
			Writer struct {
				Properties struct {
					CatalogType struct {
						Enum []string `json:"enum"`
					} `json:"catalog_type"`
				} `json:"properties"`
			} `json:"writer"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &spec))

	accepted := spec.Properties.Writer.Properties.CatalogType.Enum
	require.NotEmpty(t, accepted)
	assert.ElementsMatch(t, accepted, catalogNames())
}

func TestUpdateTypeConstraintsWithCatalog(t *testing.T) {
	testCases := []struct {
		name        string
		constraints UpdateTypeConstraints
		expected    []UpdateType
	}{
		{
			// Unity rejects identifier fields and supports neither eq nor pos delete files.
			name:        "unity offers only deletion vectors",
			constraints: UpdateTypeConstraints{Catalog: "unity"},
			expected:    []UpdateType{UpdateTypeDeletionVector},
		},
		{
			// Nessie does not support table spec v3.
			name:        "nessie drops deletion vectors",
			constraints: UpdateTypeConstraints{Catalog: "nessie"},
			expected:    []UpdateType{UpdateTypeEquality, UpdateTypePosition},
		},
		{
			// Athena reads eq and pos; Horizon rejects eq from external engines.
			name:        "engines and catalog intersect",
			constraints: UpdateTypeConstraints{Engines: []QueryEngine{QueryEngineAthena}, Catalog: "horizon"},
			expected:    []UpdateType{UpdateTypePosition},
		},
		{
			// Athena cannot read deletion vectors, the only format Unity offers; discover fails on this.
			name:        "disjoint engines and catalog leave nothing",
			constraints: UpdateTypeConstraints{Engines: []QueryEngine{QueryEngineAthena}, Catalog: "unity"},
			expected:    []UpdateType{},
		},
		{
			name:        "permissive catalog leaves the engine set",
			constraints: UpdateTypeConstraints{Engines: []QueryEngine{QueryEngineAthena}, Catalog: "glue"},
			expected:    []UpdateType{UpdateTypeEquality, UpdateTypePosition},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.constraints.Available())
		})
	}
}

// effective resolves a generated stream the way sync does: selected override, else default.
func effective(catalog *Catalog, stream *Stream) *ConfiguredStream {
	return &ConfiguredStream{Stream: stream, StreamMetadata: catalog.SelectedStreams[stream.Namespace][0]}
}

func TestGetWrappedCatalogUsesCatalogDerivedUpdateType(t *testing.T) {
	streams := []*Stream{{
		Name: "users", Namespace: "public", Schema: NewTypeSchema(),
		DefaultStreamProperties: &DefaultStreamProperties{},
	}}

	catalog := GetWrappedCatalog(streams, UpdateTypeConstraints{Catalog: "horizon"})

	// Equality would be the default, but Horizon rejects it from external engines.
	assert.Equal(t, UpdateTypePosition, streams[0].DefaultStreamProperties.UpdateType)
	assert.Equal(t, UpdateTypePosition, effective(catalog, streams[0]).GetUpdateType())
	assert.Equal(t, []UpdateType{UpdateTypePosition, UpdateTypeDeletionVector}, streams[0].AvailableUpdateTypes)
}

func TestMergeUpdateTypeClearsCatalogUnsupportedValue(t *testing.T) {
	// Configured as equality before the job pointed at a Unity destination.
	metadata := &StreamMetadata{StreamName: "users", UpdateType: new(string(UpdateTypeEquality))}

	mergeUpdateType(metadata, "public.users", UpdateTypeConstraints{Catalog: "unity"})

	assert.Equal(t, new(""), metadata.UpdateType)
}

// With no delete format left, streams stay upsert with an empty list: the UI reports the
// missing intersection and the user picks append.
func TestGetWrappedCatalogKeepsUpsertWhenNothingQualifies(t *testing.T) {
	streams := []*Stream{{
		Name: "users", Namespace: "public", Schema: NewTypeSchema(),
		DefaultStreamProperties: &DefaultStreamProperties{},
	}}

	// Athena cannot read deletion vectors, the only format Unity offers.
	catalog := GetWrappedCatalog(streams, UpdateTypeConstraints{
		Engines: []QueryEngine{QueryEngineAthena}, Catalog: "unity",
	})

	assert.False(t, effective(catalog, streams[0]).AppendModeEnabled())
	assert.False(t, streams[0].DefaultStreamProperties.AppendMode)
	assert.Equal(t, UpdateType(""), streams[0].DefaultStreamProperties.UpdateType)
	assert.Equal(t, []UpdateType{}, streams[0].AvailableUpdateTypes)
	// Sync must refuse it until the user picks append.
	require.Error(t, effective(catalog, streams[0]).ValidateUpdateType(Iceberg))
}

// Empty and absent mean opposite things: nothing qualifies vs. a catalog predating the field.
func TestAvailableUpdateTypesEmptySurvivesJSON(t *testing.T) {
	var empty Stream
	require.NoError(t, json.Unmarshal([]byte(`{"available_update_types":[]}`), &empty))
	assert.NotNil(t, empty.AvailableUpdateTypes)
	assert.Empty(t, empty.AvailableUpdateTypes)

	var legacy Stream
	require.NoError(t, json.Unmarshal([]byte(`{}`), &legacy))
	assert.Nil(t, legacy.AvailableUpdateTypes)

	out, err := json.Marshal(&Stream{AvailableUpdateTypes: []UpdateType{}})
	require.NoError(t, err)
	assert.Contains(t, string(out), `"available_update_types":[]`)
}

func TestValidateUpdateTypeWithNothingAvailable(t *testing.T) {
	testCases := []struct {
		name      string
		available []UpdateType
		metadata  StreamMetadata
		wantErr   bool
	}{
		// Append writes no deletes, so an empty list cannot block it.
		{name: "append stream passes", available: []UpdateType{}, metadata: StreamMetadata{AppendMode: new(true)}},
		{name: "upsert stream is rejected", available: []UpdateType{}, metadata: StreamMetadata{UpdateType: new("pos")}, wantErr: true},
		// Unset would otherwise default to equality and slip through.
		{name: "unset upsert stream is rejected", available: []UpdateType{}, metadata: StreamMetadata{}, wantErr: true},
		// Legacy catalogs only check writability.
		{name: "legacy nil list allows a writable mode", available: nil, metadata: StreamMetadata{UpdateType: new("pos")}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := &ConfiguredStream{
				StreamMetadata: tc.metadata,
				Stream:         &Stream{AvailableUpdateTypes: tc.available},
			}
			err := stream.ValidateUpdateType(Iceberg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// Parquet writes no delete files: streams get an empty list like "no intersection", but with
// append mode set, which is how the UI tells the two apart.
func TestGetWrappedCatalogAppendOnlyDestination(t *testing.T) {
	streams := []*Stream{{
		Name: "users", Namespace: "public", Schema: NewTypeSchema(),
		DefaultStreamProperties: &DefaultStreamProperties{},
	}}

	// Engines that would otherwise allow every format must not widen an append-only destination.
	catalog := GetWrappedCatalog(streams, UpdateTypeConstraints{
		Engines: []QueryEngine{QueryEngineSpark}, AppendOnly: true,
	})

	// Selected streams only carry overrides, so append-only shows on the defaults.
	assert.True(t, streams[0].DefaultStreamProperties.AppendMode)
	assert.True(t, effective(catalog, streams[0]).AppendModeEnabled())
	assert.Equal(t, []UpdateType{}, streams[0].AvailableUpdateTypes)
	require.NoError(t, effective(catalog, streams[0]).ValidateUpdateType(Parquet))
}

// A stream configured as upsert before the job pointed at Parquet is moved to append.
func TestMergeUpdateTypeForcesAppendOnAppendOnlyDestination(t *testing.T) {
	// The user had explicitly chosen upsert, so the override itself must flip.
	metadata := &StreamMetadata{StreamName: "users", AppendMode: new(false), UpdateType: new(string(UpdateTypePosition))}

	mergeUpdateType(metadata, "public.users", UpdateTypeConstraints{AppendOnly: true})

	assert.Equal(t, new(true), metadata.AppendMode)
	assert.Equal(t, new(""), metadata.UpdateType)
}
