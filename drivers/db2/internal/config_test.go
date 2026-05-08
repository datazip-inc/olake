package driver

import (
	"encoding/json"
	"strings"
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
				Host:     "db2-host",
				Port:     50000,
				Database: "testdb",
				Username: "db2inst1",
				Password: "secret1234",
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
				Host:     "db2-host",
				Port:     50000,
				Database: "testdb",
				Username: "db2inst1",
				Password: "secret1234",
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
				Host:     "db2-host",
				Port:     50000,
				Database: "testdb",
				Username: "db2inst1",
				Password: "secret1234",
			},
			expectErr: false,
		},
		{
			// Rejects config with an empty host.
			name: "invalid config - empty host",
			config: &Config{
				Host:     "",
				Port:     50000,
				Database: "testdb",
				Username: "db2inst1",
				Password: "secret1234",
			},
			expectErr: true,
		},
		{
			// Rejects config with an invalid port number.
			name: "invalid config - bad port",
			config: &Config{
				Host:     "db2-host",
				Port:     -1,
				Database: "testdb",
				Username: "db2inst1",
				Password: "secret1234",
			},
			expectErr: true,
		},
		{
			// Rejects config with a missing username.
			name: "invalid config - missing username",
			config: &Config{
				Host:     "db2-host",
				Port:     50000,
				Database: "testdb",
				Username: "",
				Password: "secret1234",
			},
			expectErr: true,
		},
		{
			// Rejects config with a missing database.
			name: "invalid config - missing database",
			config: &Config{
				Host:     "db2-host",
				Port:     50000,
				Database: "",
				Username: "db2inst1",
				Password: "secret1234",
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

func TestConfig_BuildTunnelDSN(t *testing.T) {
	// Ensures tunnel DSN uses localhost with the given port, preserves JDBC params, and excludes SSL.
	config := &Config{
		Host:     "db2-remote-host",
		Port:     50000,
		Database: "testdb",
		Username: "db2inst1",
		Password: "secret1234",
		JDBCURLParams: map[string]string{
			"CONNECTTIMEOUT": "30",
		},
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeRequire,
		},
	}

	dsn := config.BuildTunnelDSN(12345)

	// Tunnel DSN should point to localhost with the given port.
	if !strings.Contains(dsn, "HOSTNAME=localhost") {
		t.Errorf("Expected HOSTNAME=localhost in tunnel DSN, got: %s", dsn)
	}
	if !strings.Contains(dsn, "PORT=12345") {
		t.Errorf("Expected PORT=12345 in tunnel DSN, got: %s", dsn)
	}
	if !strings.Contains(dsn, "DATABASE=testdb") {
		t.Errorf("Expected DATABASE=testdb in tunnel DSN, got: %s", dsn)
	}

	// JDBC params should be preserved.
	if !strings.Contains(dsn, "CONNECTTIMEOUT=30") {
		t.Errorf("Expected JDBC param CONNECTTIMEOUT=30 in tunnel DSN, got: %s", dsn)
	}

	// SSL should NOT be in tunnel DSN (traffic goes through local tunnel).
	if strings.Contains(dsn, "SECURITY=SSL") {
		t.Errorf("Expected no SSL in tunnel DSN, got: %s", dsn)
	}

	// Original host should NOT appear.
	if strings.Contains(dsn, "db2-remote-host") {
		t.Errorf("Expected original host to NOT appear in tunnel DSN, got: %s", dsn)
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
				"host": "db2-host",
				"port": 50000,
				"database": "testdb",
				"username": "db2inst1",
				"password": "secret1234",
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
				"host": "db2-host",
				"port": 50000,
				"database": "testdb",
				"username": "db2inst1",
				"password": "secret1234"
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
