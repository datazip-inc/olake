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

func TestGetWrappedCatalogUsesCatalogDerivedUpdateType(t *testing.T) {
	streams := []*Stream{{Name: "users", Namespace: "public", Schema: NewTypeSchema()}}

	catalog := GetWrappedCatalog(streams, "postgres", UpdateTypeConstraints{Catalog: "horizon"})

	// Equality would be the default, but Horizon rejects it from external engines.
	assert.Equal(t, string(UpdateTypePosition), catalog.SelectedStreams["public"][0].UpdateType)
	assert.Equal(t, []UpdateType{UpdateTypePosition, UpdateTypeDeletionVector}, streams[0].AvailableUpdateTypes)
}

func TestMergeUpdateTypeClearsCatalogUnsupportedValue(t *testing.T) {
	// Configured as equality before the job pointed at a Unity destination.
	metadata := &StreamMetadata{StreamName: "users", UpdateType: string(UpdateTypeEquality)}

	mergeUpdateType(metadata, "public.users", UpdateTypeConstraints{Catalog: "unity"})

	assert.Equal(t, "", metadata.UpdateType)
}
