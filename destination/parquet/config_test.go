package parquet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidateMaxFileSizeMB(t *testing.T) {
	tests := []struct {
		name    string
		size    float64
		wantErr bool
	}{
		{
			name:    "zero is allowed (treated as unset, falls back to default)",
			size:    0,
			wantErr: false,
		},
		{
			name:    "positive value is allowed",
			size:    512,
			wantErr: false,
		},
		{
			name:    "fractional positive value is allowed",
			size:    0.125,
			wantErr: false,
		},
		{
			name:    "negative value is rejected",
			size:    -1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{MaxFileSizeMB: tt.size}
			err := c.Validate()

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "max_file_size_mb")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
