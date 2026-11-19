package destination

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/constants"
)

func TestFetchRowWithoutNormalization(t *testing.T) {
	stored := func(value any) map[string]any {
		return map[string]any{constants.StringifiedData: value}
	}

	testCases := []struct {
		name        string
		stored      any
		columns     []string
		expected    map[string]any
		expectedErr bool
	}{
		// the key's raw JSON is spliced back unchanged, and the row is parsed once for all keys
		{
			name:     "each key's raw json is returned",
			stored:   `{"payload":"v","doc":{"k":1}}`,
			columns:  []string{"payload", "doc"},
			expected: map[string]any{"payload": json.RawMessage(`"v"`), "doc": json.RawMessage(`{"k":1}`)},
		},
		// the stored row predates the column: keep the placeholder rather than invent a NULL
		{name: "a key missing from the stored row keeps the placeholder", stored: `{"id":1}`, columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		{name: "a stored JSON null keeps the placeholder", stored: `{"id":1,"payload":null}`, columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		{name: "a stored placeholder keeps the placeholder", stored: `{"id":1,"payload":"` + placeholder + `"}`, columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		// the file lacks the stored row, or the row is soft-deleted
		{name: "a NULL stored row keeps the placeholder", stored: nil, columns: []string{"payload"}, expected: map[string]any{"payload": placeholder}},
		{name: "invalid json", stored: `{not json`, columns: []string{"payload"}, expectedErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := &destinationValuesFetcher{stream: testConfiguredStream(false), read: map[string]map[string]any{}}
			err := fetcher.fetchRow(&pendingRead{olakeID: "1", columns: tc.columns}, stored(tc.stored))
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, fetcher.read["1"])
		})
	}
}
