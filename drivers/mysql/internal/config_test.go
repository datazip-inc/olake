package driver

import (
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils"
)

func TestConfig_URI_WithJDBCParams(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "testuser",
		Password: "testpass",
		Database: "testdb",
		JDBCURLParams: map[string]string{
			"charset":   "utf8mb4",
			"parseTime": "true",
			"loc":       "Local",
		},
	}

	uri := config.URI()

	// Check that JDBC params are included in the URI
	if !strings.Contains(uri, "charset=utf8mb4") {
		t.Errorf("Expected charset parameter in URI, got: %s", uri)
	}
	if !strings.Contains(uri, "parseTime=true") {
		t.Errorf("Expected parseTime parameter in URI, got: %s", uri)
	}
	if !strings.Contains(uri, "loc=Local") {
		t.Errorf("Expected loc parameter in URI, got: %s", uri)
	}
}

func TestConfig_URI_WithSSLDisabled(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "testuser",
		Password: "testpass",
		Database: "testdb",
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeDisable,
		},
	}

	uri := config.URI()

	// Check that TLS is disabled
	if !strings.Contains(uri, "tls=false") {
		t.Errorf("Expected tls=false in URI, got: %s", uri)
	}
}

func TestConfig_URI_WithSSLRequired(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "testuser",
		Password: "testpass",
		Database: "testdb",
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeRequire,
		},
	}

	uri := config.URI()

	// Check that TLS is enabled
	if !strings.Contains(uri, "tls=true") {
		t.Errorf("Expected tls=true in URI, got: %s", uri)
	}
}

func TestConfig_Validate_WithSSLConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			name: "Valid SSL config with disable mode",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeDisable,
				},
			},
			expectErr: false,
		},
		{
			name: "Valid SSL config with require mode",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeRequire,
				},
			},
			expectErr: false,
		},
		{
			name: "Invalid SSL config - verify-ca without certificates",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeVerifyCA,
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestConfig_URI_CombinedParams(t *testing.T) {
	config := &Config{
		Host:     "mysql.example.com",
		Port:     3306,
		Username: "appuser",
		Password: "securepass",
		Database: "appdb",
		JDBCURLParams: map[string]string{
			"charset":      "utf8mb4",
			"parseTime":    "true",
			"timeout":      "10s",
			"readTimeout":  "30s",
			"writeTimeout": "30s",
		},
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeRequire,
		},
	}

	uri := config.URI()

	// Verify both JDBC params and SSL config are present
	if !strings.Contains(uri, "charset=utf8mb4") {
		t.Errorf("Expected charset parameter in URI")
	}
	if !strings.Contains(uri, "tls=true") {
		t.Errorf("Expected TLS enabled in URI")
	}
	if !strings.Contains(uri, "mysql.example.com:3306") {
		t.Errorf("Expected correct host and port in URI")
	}
	if !strings.Contains(uri, "appdb") {
		t.Errorf("Expected database name in URI")
	}
}
