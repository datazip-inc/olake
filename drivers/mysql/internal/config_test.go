package driver

import (
	"crypto/tls"
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
		// Ensures JDBC URL parameters are propagated into the DSN.
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
		// Confirms TLS is disabled when mode is disable.
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
		// Confirms TLS uses skip-verify for require mode.
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
		// Verifies JDBC params and TLS are both applied together.
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
		// Rejects invalid CA data and marks URI as invalid without falling back to skip-verify.
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
		// Rejects invalid client cert/key and does not fall back to skip-verify.
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
		// Uses verify-full (verify-identity) with provided CA and client credentials and keeps TLS config name unique.
		{
			name: "with verify-full and valid certificates",
			config: func() *Config {
				certs := generateTestCerts()
				return &Config{
					Host:     "db.internal",
					Port:     3306,
					Username: "secureuser",
					Password: "securepass",
					Database: "secured",
					SSLConfiguration: &utils.SSLConfig{
						Mode:       utils.SSLModeVerifyFull,
						ServerCA:   certs.CACert,
						ClientCert: certs.ClientCert,
						ClientKey:  certs.ClientKey,
					},
				}
			}(),
			expectedContains: []string{
				"db.internal:3306",
				"secured",
				"tls=mysql_",
			},
			notExpected: []string{
				"invalid-ssl-config:0",
				"tls=skip-verify",
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
		// Accepts a minimal valid config with SSL disabled.
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
		// Accepts a valid config with SSL required.
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
		// Rejects verify-ca when certificates are missing.
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
		// Rejects empty host value.
		{
			name: "invalid config - empty host",
			config: &Config{
				Host:     "",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
			},
			expectErr: true,
		},
		// Rejects host containing protocol scheme.
		{
			name: "invalid config - host with scheme",
			config: &Config{
				Host:     "https://localhost",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
			},
			expectErr: true,
		},
		// Rejects invalid port range.
		{
			name: "invalid config - bad port",
			config: &Config{
				Host:     "localhost",
				Port:     -1,
				Username: "testuser",
				Password: "testpass",
			},
			expectErr: true,
		},
		// Rejects missing username.
		{
			name: "invalid config - missing username",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "",
				Password: "testpass",
			},
			expectErr: true,
		},
		// Rejects missing password.
		{
			name: "invalid config - missing password",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "testuser",
				Password: "",
			},
			expectErr: true,
		},
		// Defaults database to mysql when empty.
		{
			name: "valid config - defaults database",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				Username: "testuser",
				Password: "testpass",
				Database: "",
			},
			expectErr: false,
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
			if !tt.expectErr && err == nil && tt.config.Database == "" {
				t.Errorf("Expected database to default to 'mysql'")
			}
		})
	}
}

func TestConfig_buildTLSConfig(t *testing.T) {
	certs := generateTestCerts()

	tests := []struct {
		name        string
		config      *Config
		expectErr   bool
		assertions  func(t *testing.T, tlsCfg *tls.Config)
		description string
	}{
		// Builds require mode with simple skip-verify config.
		{
			name:      "require builds with InsecureSkipVerify",
			config:    &Config{SSLConfiguration: &utils.SSLConfig{Mode: utils.SSLModeRequire}},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if !tlsCfg.InsecureSkipVerify {
					t.Fatalf("expected InsecureSkipVerify to be true for require mode")
				}
				if tlsCfg.RootCAs != nil {
					t.Fatalf("expected RootCAs to be nil for require mode (no CA verification)")
				}
				if tlsCfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("expected MinVersion to be TLS 1.2")
				}
			},
		},
		// Builds verify-ca config enabling custom verification callback.
		{
			name:      "verify-ca builds with CA and callback",
			config:    &Config{SSLConfiguration: &utils.SSLConfig{Mode: utils.SSLModeVerifyCA, ServerCA: certs.CACert}},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if !tlsCfg.InsecureSkipVerify {
					t.Fatalf("expected InsecureSkipVerify to be true for verify-ca")
				}
				if tlsCfg.VerifyPeerCertificate == nil {
					t.Fatalf("expected VerifyPeerCertificate callback to be set")
				}
			},
		},
		// Builds verify-full config with client auth certificates.
		{
			name:      "verify-full builds with client certs",
			config:    &Config{SSLConfiguration: &utils.SSLConfig{Mode: utils.SSLModeVerifyFull, ServerCA: certs.CACert, ClientCert: certs.ClientCert, ClientKey: certs.ClientKey}},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if tlsCfg.InsecureSkipVerify {
					t.Fatalf("expected InsecureSkipVerify to be false for verify-full")
				}
				if len(tlsCfg.Certificates) == 0 {
					t.Fatalf("expected client certificates to be loaded")
				}
			},
		},
		// Returns an error when CA PEM is invalid.
		{
			name:       "fails with invalid CA pem",
			config:     &Config{SSLConfiguration: &utils.SSLConfig{Mode: utils.SSLModeVerifyCA, ServerCA: "not-a-pem"}},
			expectErr:  true,
			assertions: func(t *testing.T, _ *tls.Config) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tlsCfg, err := tt.config.buildTLSConfig()
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}
			if tlsCfg == nil {
				t.Fatalf("expected TLS config to be returned")
			}
			tt.assertions(t, tlsCfg)
		})
	}
}
