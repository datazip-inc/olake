package driver

import (
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils"
)

func TestConfig_URI(t *testing.T) {
	tests := []struct {
		name             string
		config           *Config
		expectedContains []string
		notExpected      []string
	}{
		{
			name: "with JDBC parameters",
			config: &Config{
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
			},
			expectedContains: []string{
				"charset=utf8mb4",
				"parseTime=true",
				"loc=Local",
			},
		},
		{
			name: "with SSL disabled",
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
			expectedContains: []string{
				"tls=false",
			},
		},
		{
			name: "with SSL required",
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
			expectedContains: []string{
				"tls=skip-verify",
			},
		},
		{
			name: "with combined JDBC params and SSL",
			config: &Config{
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
			},
			expectedContains: []string{
				"charset=utf8mb4",
				"tls=skip-verify",
				"mysql.example.com:3306",
				"appdb",
			},
		},
		{
			name: "with invalid CA certificate",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: "-----BEGIN CERTIFICATE-----\nINVALID_CERTIFICATE_DATA\n-----END CERTIFICATE-----",
				},
			},
			expectedContains: []string{
				"invalid-ssl-config:0",
			},
			notExpected: []string{
				"skip-verify",
			},
		},
		{
			name: "with invalid client certificate",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
				Database: "testdb",
				SSLConfiguration: &utils.SSLConfig{
					Mode:       utils.SSLModeVerifyFull,
					ServerCA:   "-----BEGIN CERTIFICATE-----\nMIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF\n-----END CERTIFICATE-----",
					ClientCert: "not a certificate",
					ClientKey:  "not a key",
				},
			},
			expectedContains: []string{
				"invalid-ssl-config:0",
			},
			notExpected: []string{
				"skip-verify",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri := tt.config.URI()

			for _, expected := range tt.expectedContains {
				if !strings.Contains(uri, expected) {
					t.Errorf("Expected URI to contain '%s', got: %s", expected, uri)
				}
			}

			for _, notExpected := range tt.notExpected {
				if strings.Contains(uri, notExpected) {
					t.Errorf("Expected URI to NOT contain '%s', got: %s", notExpected, uri)
				}
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			name: "valid config with SSL disabled",
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
			name: "valid config with SSL required",
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
			name: "invalid config - verify-ca without certificates",
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
