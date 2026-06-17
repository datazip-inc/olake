package driver

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			// Accepts a valid config with SSH password authentication.
			name: "valid config with SSH password auth",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
				SSHConfig: &utils.SSHConfig{
					Host:     "bastion",
					Port:     22,
					Username: "sshuser",
					Password: "sshpass",
				},
			},
			expectErr: false,
		},
		{
			// Accepts a valid config with SSH key authentication.
			name: "valid config with SSH key auth",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
				SSHConfig: &utils.SSHConfig{
					Host:       "bastion",
					Port:       22,
					Username:   "sshuser",
					PrivateKey: "-----BEGIN OPENSSH PRIVATE KEY-----\ntest\n-----END OPENSSH PRIVATE KEY-----",
				},
			},
			expectErr: false,
		},
		{
			// Accepts a valid config without SSH.
			name: "valid config without SSH",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
			},
			expectErr: false,
		},
		{
			// Rejects config with an empty host.
			name: "invalid config - empty host",
			config: &Config{
				Host:     "",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
			},
			expectErr: true,
		},
		{
			// Rejects config when host contains a URL scheme.
			name: "invalid config - host with scheme",
			config: &Config{
				Host:     "https://mssql-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
			},
			expectErr: true,
		},
		{
			// Rejects config with an invalid port number.
			name: "invalid config - bad port",
			config: &Config{
				Host:     "mssql-host",
				Port:     -1,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
			},
			expectErr: true,
		},
		{
			// Rejects config with a missing username.
			name: "invalid config - missing username",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "testdb",
				Username: "",
				Password: "Password!123",
			},
			expectErr: true,
		},
		{
			// Rejects config with a missing password.
			name: "invalid config - missing password",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "",
			},
			expectErr: true,
		},
		{
			// Rejects config with a missing database.
			name: "invalid config - missing database",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "",
				Username: "sa",
				Password: "Password!123",
			},
			expectErr: true,
		},
		{
			name: "valid config with primary_config",
			config: &Config{
				Host:     "replica-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
				PrimaryConfig: &PrimaryConfig{
					Host:     "primary-host",
					Port:     1433,
					Username: "sa",
					Password: "PrimaryPass!123",
				},
			},
			expectErr: false,
		},
		{
			name: "valid config with manage_capture_instances and no primary_config",
			config: &Config{
				Host:                   "replica-host",
				Port:                   1433,
				Database:               "testdb",
				Username:               "sa",
				Password:               "Password!123",
				ManageCaptureInstances: true,
			},
			expectErr: false,
		},
		{
			name: "invalid primary_config - empty host",
			config: &Config{
				Host:     "replica-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
				PrimaryConfig: &PrimaryConfig{
					Host:     "",
					Port:     1433,
					Username: "sa",
					Password: "PrimaryPass!123",
				},
			},
			expectErr: true,
		},
		{
			name: "invalid primary_config - bad port",
			config: &Config{
				Host:     "replica-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
				PrimaryConfig: &PrimaryConfig{
					Host:     "primary-host",
					Port:     -1,
					Username: "sa",
					Password: "PrimaryPass!123",
				},
			},
			expectErr: true,
		},
		{
			name: "invalid primary_config - missing password",
			config: &Config{
				Host:     "replica-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
				PrimaryConfig: &PrimaryConfig{
					Host:     "primary-host",
					Port:     1433,
					Username: "sa",
					Password: "",
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

func TestConfig_URI(t *testing.T) {
	tests := []struct {
		name             string
		config           *Config
		expectError      bool
		expectedContains []string
		notExpected      []string
	}{
		{
			name: "with jdbc url params",
			config: func() *Config {
				certs := testutils.GenerateTestCerts()
				return &Config{
					Host:     "mssql-host",
					Port:     1433,
					Database: "testdb",
					Username: "sa",
					Password: "Password!123",
					JDBCURLParams: map[string]string{
						"ApplicationIntent":   "ReadOnly",
						"MultiSubnetFailover": "true",
					},
					SSLConfiguration: &utils.SSLConfig{
						Mode:       utils.SSLModeVerifyFull,
						ServerCA:   certs.CACert,
						ClientCert: certs.ClientCert,
						ClientKey:  certs.ClientKey,
					},
				}
			}(),
			expectedContains: []string{
				"mssql-host:1433",
				"ApplicationIntent=ReadOnly",
				"MultiSubnetFailover=true",
			},
		},
		{
			name: "config database overrides jdbc_url_params database",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "realdb",
				Username: "sa",
				Password: "Password!123",
				JDBCURLParams: map[string]string{
					"database": "wrongdb",
				},
			},
			expectedContains: []string{
				"mssql-host:1433",
				"database=realdb",
			},
			notExpected: []string{
				"database=wrongdb",
			},
		},
		{
			name: "ssl mode overrides jdbc_url_params encrypt",
			config: &Config{
				Host:     "mssql-host",
				Port:     1433,
				Database: "testdb",
				Username: "sa",
				Password: "Password!123",
				JDBCURLParams: map[string]string{
					"encrypt": "true",
				},
				SSLConfiguration: &utils.SSLConfig{Mode: utils.SSLModeDisable},
			},
			expectedContains: []string{
				"mssql-host:1433",
				"encrypt=disable",
			},
		},
		{
			name: "ssl require sets encrypt and trust server certificate",
			config: &Config{
				Host:             "mssql-host",
				Port:             1433,
				Database:         "testdb",
				Username:         "sa",
				Password:         "Password!123",
				SSLConfiguration: &utils.SSLConfig{Mode: utils.SSLModeRequire},
			},
			expectedContains: []string{
				"mssql-host:1433",
				"encrypt=true",
				"TrustServerCertificate=true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("failed to validate config: %v", err)
				return
			}

			connStr := tt.config.URI()

			for _, expected := range tt.expectedContains {
				if !strings.Contains(connStr, expected) {
					t.Errorf("expected connection string to contain %q, got %s", expected, connStr)
				}
			}
			for _, notExpected := range tt.notExpected {
				if strings.Contains(connStr, notExpected) {
					t.Errorf("expected connection string to NOT contain %q, got %s", notExpected, connStr)
				}
			}
		})
	}
}

func TestConfig_PrimaryURI(t *testing.T) {
	config := &Config{
		Host:     "replica-host",
		Port:     1433,
		Database: "testdb",
		Username: "replica_user",
		Password: "ReplicaPass!123",
		JDBCURLParams: map[string]string{
			"ApplicationIntent": "ReadOnly",
		},
		SSLConfiguration: &utils.SSLConfig{Mode: utils.SSLModeRequire},
		PrimaryConfig: &PrimaryConfig{
			Host:     "primary-host",
			Port:     1433,
			Username: "primary_user",
			Password: "PrimaryPass!123",
		},
	}

	require.NoError(t, config.Validate())

	connStr := config.primaryURI()

	for _, expected := range []string{
		"primary-host:1433",
		"database=testdb",
		"ApplicationIntent=ReadOnly",
		"encrypt=true",
		"TrustServerCertificate=true",
	} {
		if !strings.Contains(connStr, expected) {
			t.Errorf("expected primary URI to contain %q, got %s", expected, connStr)
		}
	}

	if strings.Contains(connStr, "replica-host") {
		t.Errorf("expected primary URI to not contain replica host, got %s", connStr)
	}
}

func TestConfig_SSHConfigDeserialization(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		wantSSH  bool
		wantHost string
		wantPort int
	}{
		{
			// Correctly deserializes SSH password config from JSON.
			name: "with SSH password config",
			jsonData: `{
				"host": "mssql-host",
				"port": 1433,
				"database": "testdb",
				"username": "sa",
				"password": "Password!123",
				"ssh_config": {
					"host": "bastion",
					"port": 22,
					"username": "sshuser",
					"password": "sshpass"
				}
			}`,
			wantSSH:  true,
			wantHost: "bastion",
			wantPort: 22,
		},
		{
			// Handles missing SSH config gracefully.
			name: "without SSH config",
			jsonData: `{
				"host": "mssql-host",
				"port": 1433,
				"database": "testdb",
				"username": "sa",
				"password": "Password!123"
			}`,
			wantSSH: false,
		},
		{
			name: "with primary_config",
			jsonData: `{
				"host": "replica-host",
				"port": 1433,
				"database": "testdb",
				"username": "sa",
				"password": "Password!123",
				"ssh_config": {
					"host": "bastion",
					"port": 22,
					"username": "sshuser",
					"password": "sshpass"
				},
				"primary_config": {
					"host": "primary-host",
					"port": 1433,
					"username": "sa",
					"password": "PrimaryPass!123"
				}
			}`,
			wantSSH:  true,
			wantHost: "bastion",
			wantPort: 22,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config Config
			if err := json.Unmarshal([]byte(tt.jsonData), &config); err != nil {
				t.Fatalf("Failed to unmarshal config: %v", err)
			}

			if tt.wantSSH {
				if config.SSHConfig == nil {
					t.Fatal("Expected SSH config to be present")
				}
				if config.SSHConfig.Host != tt.wantHost {
					t.Errorf("Expected SSH host %q, got %q", tt.wantHost, config.SSHConfig.Host)
				}
				if config.SSHConfig.Port != tt.wantPort {
					t.Errorf("Expected SSH port %d, got %d", tt.wantPort, config.SSHConfig.Port)
				}
			} else {
				if config.SSHConfig != nil {
					t.Error("Expected SSH config to be nil")
				}
			}

			if strings.Contains(tt.name, "primary_config") {
				if config.PrimaryConfig == nil {
					t.Fatal("Expected primary_config to be present")
				}
				if config.PrimaryConfig.Host != "primary-host" {
					t.Errorf("Expected primary host primary-host, got %q", config.PrimaryConfig.Host)
				}
			}
		})
	}
}
