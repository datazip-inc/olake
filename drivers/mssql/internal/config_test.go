package driver

import (
	"encoding/json"
	"testing"

	"github.com/datazip-inc/olake/utils"
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
		})
	}
}
