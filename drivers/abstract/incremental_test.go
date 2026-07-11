package abstract

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsMetadataCursorAhead(t *testing.T) {
	tests := []struct {
		name              string
		metadataPrimary   any
		metadataSecondary any
		statePrimary      any
		stateSecondary    any
		hasSecondary      bool
		expected          bool
	}{
		{name: "primary cursor ahead", metadataPrimary: 20, statePrimary: 10, expected: true},
		{name: "secondary cursor ahead", metadataPrimary: 10, metadataSecondary: 20, statePrimary: 10, stateSecondary: 10, hasSecondary: true, expected: true},
		{name: "cursor equal", metadataPrimary: 10, metadataSecondary: 10, statePrimary: 10, stateSecondary: 10, hasSecondary: true},
		{name: "metadata cursor behind", metadataPrimary: 5, metadataSecondary: 20, statePrimary: 10, stateSecondary: 10, hasSecondary: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := isMetadataCursorAhead(tt.metadataPrimary, tt.metadataSecondary, tt.statePrimary, tt.stateSecondary, tt.hasSecondary)
			require.Equal(t, tt.expected, actual)
		})
	}
}
