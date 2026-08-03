package iceberg

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidateDeleteMode(t *testing.T) {
	tests := []struct {
		name           string
		deleteMode     types.DeleteMode
		useArrowWrites bool
		wantMode       types.DeleteMode
		wantErr        string
	}{
		{
			name:     "defaults to equality deletes",
			wantMode: types.DeleteModeEquality,
		},
		{
			name:       "equality deletes need no arrow writes",
			deleteMode: types.DeleteModeEquality,
			wantMode:   types.DeleteModeEquality,
		},
		{
			name:           "position deletes accepted with arrow writes",
			deleteMode:     types.DeleteModePosition,
			useArrowWrites: true,
			wantMode:       types.DeleteModePosition,
		},
		{
			name:       "position deletes rejected without arrow writes",
			deleteMode: types.DeleteModePosition,
			wantErr:    "requires arrow_writes",
		},
		{
			name:       "deletion vectors not implemented",
			deleteMode: types.DeleteModeDeletionVector,
			wantErr:    "not implemented yet",
		},
		{
			name:       "unknown mode rejected",
			deleteMode: types.DeleteMode("merge-on-read"),
			wantErr:    "unsupported delete_mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				IcebergS3Path:  "s3://warehouse",
				CatalogType:    JDBCCatalog,
				JDBCUrl:        "jdbc:postgresql://localhost:5432/iceberg",
				DeleteMode:     tt.deleteMode,
				UseArrowWrites: tt.useArrowWrites,
				// Skip the JAR discovery that Validate would otherwise perform.
				JarPath: "/tmp/olake-iceberg-java-writer.jar",
			}

			err := config.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, config.DeleteMode)
		})
	}
}

func TestDeleteModeNeedsRowIndex(t *testing.T) {
	assert.False(t, types.DeleteModeEquality.NeedsRowIndex())
	assert.True(t, types.DeleteModePosition.NeedsRowIndex())
}
