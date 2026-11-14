package iceberg

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccessDelegationHeader(t *testing.T) {
	const headerKey = "header.X-Iceberg-Access-Delegation"

	testCases := []struct {
		name       string
		catalog    CatalogType
		configured string
		expected   string
	}{
		{
			name:     "biglake requests vended credentials",
			catalog:  "biglake",
			expected: "vended-credentials",
		},
		{
			name:     "horizon requests vended credentials",
			catalog:  "horizon",
			expected: "vended-credentials",
		},
		{
			name:    "generic rest sends no header by default",
			catalog: RestCatalog,
		},
		{
			name:       "generic rest keeps a configured value",
			catalog:    RestCatalog,
			configured: "vended-credentials",
			expected:   "vended-credentials",
		},
		{
			name:    "polaris sends no header by default",
			catalog: "polaris",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &Config{
				CatalogType:          tc.catalog,
				IcebergS3Path:        "gs://test-bucket",
				RestCatalogURL:       "https://catalog.example.com",
				RESTAccessDelegation: tc.configured,
				JarPath:              "olake-iceberg-java-writer.jar",
			}
			require.NoError(t, config.Validate())

			raw, err := getServerConfigJSON(config, defaultServerPort, false, "")
			require.NoError(t, err)

			var serverConfig map[string]any
			require.NoError(t, json.Unmarshal(raw, &serverConfig))

			header, sent := serverConfig[headerKey]
			if tc.expected == "" {
				assert.False(t, sent, "expected no %s, got %v", headerKey, header)
				return
			}
			assert.Equal(t, tc.expected, header)
		})
	}
}
