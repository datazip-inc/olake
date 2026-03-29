package driver

import (
	"crypto/tls"
	"strings"
	"testing"
)

func TestMongo_BuildTLSConfig(t *testing.T) {
	certs := generateTestCerts()

	tests := []struct {
		name       string
		config     *Config
		expectErr  bool
		assertions func(t *testing.T, tlsCfg *tls.Config)
	}{
		{
			name: "TLS disabled returns minimal config",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode: "disable",
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if tlsCfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("expected MinVersion to be TLS 1.2")
				}
			},
		},
		{
			name: "TLS require returns config without CA verification",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode: "require",
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if tlsCfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("expected MinVersion to be TLS 1.2")
				}
				if tlsCfg.RootCAs != nil {
					t.Fatalf("expected RootCAs to be nil for require mode")
				}
			},
		},
		{
			name: "TLS verify-ca with CA certificate",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode:     "verify-ca",
					ServerCA: certs.CACert,
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if tlsCfg.RootCAs == nil {
					t.Fatalf("expected RootCAs to be set for verify-ca mode")
				}
				if tlsCfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("expected MinVersion to be TLS 1.2")
				}
			},
		},
		{
			name: "TLS verify-full with CA and client certificates",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode:       "verify-full",
					ServerCA:   certs.CACert,
					ClientCert: certs.ClientCert,
					ClientKey:  certs.ClientKey,
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if tlsCfg.RootCAs == nil {
					t.Fatalf("expected RootCAs to be set for verify-full mode")
				}
				if len(tlsCfg.Certificates) == 0 {
					t.Fatalf("expected client certificates to be loaded")
				}
				if tlsCfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("expected MinVersion to be TLS 1.2")
				}
			},
		},
		{
			name: "TLS with only client cert and key",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode:       "require",
					ClientCert: certs.ClientCert,
					ClientKey:  certs.ClientKey,
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if len(tlsCfg.Certificates) == 0 {
					t.Fatalf("expected client certificates to be loaded")
				}
			},
		},
		{
			name: "TLS with invalid CA PEM returns error",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode:     "verify-ca",
					ServerCA: "not-a-valid-pem",
				},
			},
			expectErr: true,
			assertions: func(t *testing.T, _ *tls.Config) {
			},
		},
		{
			name: "TLS with invalid client certificate returns error",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode:       "require",
					ClientCert: "not-a-valid-cert",
					ClientKey:  certs.ClientKey,
				},
			},
			expectErr: true,
			assertions: func(t *testing.T, _ *tls.Config) {
			},
		},
		{
			name: "TLS with invalid client key returns error",
			config: &Config{
				TLSConfig: TLSConfig{
					Mode:       "require",
					ClientCert: certs.ClientCert,
					ClientKey:  "not-a-valid-key",
				},
			},
			expectErr: true,
			assertions: func(t *testing.T, _ *tls.Config) {
			},
		},
		{
			name: "empty TLS config returns minimal config",
			config: &Config{
				TLSConfig: TLSConfig{},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if tlsCfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("expected MinVersion to be TLS 1.2")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mongo := &Mongo{config: tt.config}
			tlsCfg, err := mongo.BuildTLSConfig()

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

func TestConfig_URI(t *testing.T) {
	tests := []struct {
		name             string
		config           *Config
		expectedContains []string
		notExpected      []string
	}{
		{
			name: "default SCRAM-SHA-1 auth with username and password",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Username:      "testuser",
				Password:      "testpass",
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismSCRAMSHA1,
			},
			expectedContains: []string{
				"mongodb://testuser:testpass@localhost:27017/",
				"authSource=admin",
				"authMechanism=SCRAM-SHA-1",
			},
		},
		{
			name: "SCRAM-SHA-256 auth with explicit mechanism",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Username:      "testuser",
				Password:      "testpass",
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismSCRAMSHA256,
			},
			expectedContains: []string{
				"mongodb://testuser:testpass@localhost:27017/",
				"authSource=admin",
				"authMechanism=SCRAM-SHA-256",
			},
		},
		{
			name: "X509 auth with external auth source",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismX509,
			},
			expectedContains: []string{
				"mongodb://localhost:27017/",
				"authSource=%24external",
				"authMechanism=MONGODB-X509",
			},
			notExpected: []string{
				"authSource=admin",
			},
		},
		{
			name: "PLAIN auth with external auth source",
			config: &Config{
				Hosts:         []string{"ldap.example.com:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismPLAINTEXT,
			},
			expectedContains: []string{
				"mongodb://ldap.example.com:27017/",
				"authSource=%24external",
				"authMechanism=PLAIN",
			},
		},
		{
			name: "GSSAPI auth with external auth source",
			config: &Config{
				Hosts:         []string{"kerberos.example.com:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismGSSAPI,
			},
			expectedContains: []string{
				"mongodb://kerberos.example.com:27017/",
				"authSource=%24external",
				"authMechanism=GSSAPI",
			},
		},
		{
			name: "MONGODB-AWS auth with external auth source",
			config: &Config{
				Hosts:         []string{"aws.mongodb.example.com:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismMONGOAWS,
			},
			expectedContains: []string{
				"mongodb://aws.mongodb.example.com:27017/",
				"authSource=%24external",
				"authMechanism=MONGO-AWS",
			},
		},
		{
			name: "default auth without explicit mechanism",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
			},
			expectedContains: []string{
				"mongodb://testuser:testpass@localhost:27017/",
				"authSource=admin",
			},
			notExpected: []string{
				"authMechanism=",
			},
		},
		{
			name: "SRV connection string",
			config: &Config{
				Hosts:    []string{"cluster0.mongodb.net"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				Srv:      true,
			},
			expectedContains: []string{
				"mongodb+srv://testuser:testpass@cluster0.mongodb.net/",
				"authSource=admin",
			},
		},
		{
			name: "replica set configuration",
			config: &Config{
				Hosts:      []string{"rs1:27017,rs2:27017,rs3:27017"},
				Username:   "testuser",
				Password:   "testpass",
				AuthDB:     "admin",
				Database:   "testdb",
				ReplicaSet: "rs0",
			},
			expectedContains: []string{
				"mongodb://testuser:testpass@rs1:27017,rs2:27017,rs3:27017/",
				"replicaSet=rs0",
				"readPreference=secondaryPreferred",
			},
		},
		{
			name: "with additional params",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				AdditionalParams: map[string]string{
					"authSource": "custom",
				},
			},
			expectedContains: []string{
				"authSource=custom",
			},
			notExpected: []string{
				"authSource=admin",
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
			name: "valid config with SCRAM-SHA-1",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Username:      "testuser",
				Password:      "testpass",
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismSCRAMSHA1,
			},
			expectErr: false,
		},
		{
			name: "valid config with SCRAM-SHA-256",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Username:      "testuser",
				Password:      "testpass",
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismSCRAMSHA256,
			},
			expectErr: false,
		},
		{
			name: "valid config with X509 and username",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Username:      "CN=myClient,OU=Engineering,O=MyOrg",
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismX509,
			},
			expectErr: false,
		},
		{
			name: "invalid config - X509 without username",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismX509,
			},
			expectErr: true,
		},
		{
			name: "valid config with PLAIN (LDAP)",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismPLAINTEXT,
			},
			expectErr: false,
		},
		{
			name: "valid config with GSSAPI (Kerberos)",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismGSSAPI,
			},
			expectErr: false,
		},
		{
			name: "valid config with MONGODB-AWS",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				AuthDB:        "admin",
				Database:      "testdb",
				AuthMechanism: AuthMechanismMONGOAWS,
			},
			expectErr: false,
		},
		{
			name: "valid config with TLS disabled",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				TLSConfig: TLSConfig{
					Mode: "disable",
				},
			},
			expectErr: false,
		},
		{
			name: "valid config with TLS require",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				TLSConfig: TLSConfig{
					Mode: "require",
				},
			},
			expectErr: false,
		},
		{
			name: "valid config with TLS verify-ca and CA cert",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				TLSConfig: TLSConfig{
					Mode:     "verify-ca",
					ServerCA: generateTestCerts().CACert,
				},
			},
			expectErr: false,
		},
		{
			name: "invalid config - TLS verify-ca without CA cert",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				TLSConfig: TLSConfig{
					Mode: "verify-ca",
				},
			},
			expectErr: true,
		},
		{
			name: "invalid config - TLS with client cert but no key",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				TLSConfig: TLSConfig{
					Mode:       "require",
					ClientCert: "cert",
				},
			},
			expectErr: true,
		},
		{
			name: "invalid config - TLS with client key but no cert",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Username: "testuser",
				Password: "testpass",
				AuthDB:   "admin",
				Database: "testdb",
				TLSConfig: TLSConfig{
					Mode:      "require",
					ClientKey: "key",
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}
