package driver

import (
	"crypto/tls"
	"net/url"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestConfig_URI(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name           string
		config         *Config
		expectedScheme string
		expectedHost   string
		expectedQuery  map[string]string
		absentQuery    []string
	}{
		{
			name: "strips tls file params when ssl enabled",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: certs.CACert,
				},
				AdditionalParams: map[string]string{
					"tls":                   "true",
					"tlsCAFile":             "/certs/root-ca.crt",
					"tlsCertificateKeyFile": "/certs/client.pem",
					"connectTimeoutMS":      "5000",
				},
			},
			expectedScheme: "mongodb",
			expectedHost:   "mongo.example.com:27017",
			expectedQuery: map[string]string{
				"tls":              "true",
				"connectTimeoutMS": "5000",
				"authSource":       "admin",
			},
			absentQuery: []string{"tlsCAFile", "tlsCertificateKeyFile"},
		},
		{
			name: "preserves tls file params without ssl",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
				AdditionalParams: map[string]string{
					"tls":       "true",
					"tlsCAFile": "/certs/root-ca.crt",
				},
			},
			expectedScheme: "mongodb",
			expectedHost:   "mongo.example.com:27017",
			expectedQuery: map[string]string{
				"tls":        "true",
				"tlsCAFile":  "/certs/root-ca.crt",
				"authSource": "admin",
			},
		},
		{
			name: "adds tls when ssl enabled",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: certs.CACert,
				},
			},
			expectedScheme: "mongodb",
			expectedHost:   "mongo.example.com:27017",
			expectedQuery: map[string]string{
				"tls":        "true",
				"authSource": "admin",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri := tt.config.URI()
			parsed, err := url.Parse(uri)
			if err != nil {
				t.Fatalf("parse uri: %v", err)
			}
			if parsed.Scheme != tt.expectedScheme {
				t.Fatalf("scheme = %q, want %q (uri: %s)", parsed.Scheme, tt.expectedScheme, uri)
			}
			if parsed.Host != tt.expectedHost {
				t.Fatalf("host = %q, want %q (uri: %s)", parsed.Host, tt.expectedHost, uri)
			}

			query := parsed.Query()
			for k, v := range tt.expectedQuery {
				if got := query.Get(k); got != v {
					t.Fatalf("query[%q] = %q, want %q (uri: %s)", k, got, v, uri)
				}
			}
			for _, k := range tt.absentQuery {
				if query.Get(k) != "" {
					t.Fatalf("query[%q] should be absent, got %q (uri: %s)", k, query.Get(k), uri)
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
		after     func(t *testing.T, cfg *Config)
	}{
		{
			name: "empty hosts",
			config: &Config{
				Hosts:    []string{},
				Database: "testdb",
				Username: "user",
				Password: "pass",
				AuthDB:   "admin",
			},
			expectErr: true,
		},
		{
			name: "empty hosts with srv",
			config: &Config{
				Hosts:    []string{},
				Srv:      true,
				Database: "testdb",
				Username: "user",
				Password: "pass",
				AuthDB:   "admin",
			},
			expectErr: true,
		},
		{
			name: "missing database",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Username: "user",
				Password: "pass",
				AuthDB:   "admin",
			},
			expectErr: true,
		},
		{
			name: "missing auth fields",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
			},
			expectErr: true,
		},
		{
			name: "verify-ca requires server ca",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
				Username: "user",
				Password: "pass",
				AuthDB:   "admin",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeVerifyCA,
				},
			},
			expectErr: true,
		},
		{
			name: "sets defaults",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
				Username: "user",
				Password: "pass",
				AuthDB:   "admin",
			},
			expectErr: false,
			after: func(t *testing.T, cfg *Config) {
				if cfg.MaxThreads == 0 {
					t.Fatalf("expected MaxThreads default to be set")
				}
				if cfg.RetryCount == 0 {
					t.Fatalf("expected RetryCount default to be set")
				}
				if cfg.SSLConfiguration == nil || cfg.SSLConfiguration.Mode != utils.SSLModeDisable {
					t.Fatalf("expected default ssl mode disable, got %#v", cfg.SSLConfiguration)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}
			if tt.after != nil {
				tt.after(t, tt.config)
			}
		})
	}
}

func TestConfig_buildTLSConfig(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name       string
		config     *Config
		wantNil    bool
		assertions func(t *testing.T, tlsCfg *tls.Config)
	}{
		{
			name: "no ssl returns nil",
			config: &Config{
				Hosts: []string{"mongo.example.com:27017"},
			},
			wantNil: true,
		},
		{
			name: "ssl disabled returns nil",
			config: &Config{
				Hosts: []string{"mongo.example.com:27017"},
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeDisable,
				},
			},
			wantNil: true,
		},
		{
			name: "verify-ca uses verify-ca semantics",
			config: &Config{
				Hosts: []string{"mongo.example.com:27017"},
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: certs.CACert,
				},
			},
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if !tlsCfg.InsecureSkipVerify {
					t.Fatalf("expected verify-ca to skip hostname verification")
				}
				if tlsCfg.VerifyPeerCertificate == nil {
					t.Fatalf("expected custom peer certificate verification")
				}
				if tlsCfg.RootCAs == nil {
					t.Fatalf("expected root CAs to be configured")
				}
			},
		},
		{
			name: "client cert enables mTLS",
			config: &Config{
				Hosts: []string{"mongo.example.com:27017"},
				SSLConfiguration: &utils.SSLConfig{
					Mode:       utils.SSLModeVerifyCA,
					ServerCA:   certs.CACert,
					ClientCert: certs.ClientCert,
					ClientKey:  certs.ClientKey,
				},
			},
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if len(tlsCfg.Certificates) == 0 {
					t.Fatalf("expected client certificate to be configured")
				}
			},
		},
		{
			name: "verify-full leaves ServerName empty for single host",
			config: &Config{
				Hosts: []string{"mongo1.internal:27017"},
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyFull,
					ServerCA: certs.CACert,
				},
			},
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				assertVerifyFullEmptyServerName(t, tlsCfg)
			},
		},
		{
			name: "verify-full leaves ServerName empty for multi host",
			config: &Config{
				Hosts: []string{
					"mongo1.internal:27017",
					"mongo2.internal:27017",
					"mongo3.internal:27017",
				},
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyFull,
					ServerCA: certs.CACert,
				},
			},
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				assertVerifyFullEmptyServerName(t, tlsCfg)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tlsCfg, err := tt.config.buildTLSConfig()
			if err != nil {
				t.Fatalf("buildTLSConfig() error = %v", err)
			}
			if tt.wantNil {
				if tlsCfg != nil {
					t.Fatalf("expected nil tls config, got %#v", tlsCfg)
				}
				return
			}
			if tlsCfg == nil {
				t.Fatalf("expected tls config, got nil")
			}
			tt.assertions(t, tlsCfg)
		})
	}
}

func assertVerifyFullEmptyServerName(t *testing.T, tlsCfg *tls.Config) {
	t.Helper()
	if tlsCfg.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify=false for verify-full")
	}
	if tlsCfg.ServerName != "" {
		t.Fatalf("expected empty ServerName so mongo-driver can set dialed hostname, got %q", tlsCfg.ServerName)
	}
	if tlsCfg.RootCAs == nil {
		t.Fatalf("expected root CAs to be configured")
	}
	if tlsCfg.VerifyPeerCertificate != nil {
		t.Fatalf("expected no custom VerifyPeerCertificate; hostname verify is left to the driver")
	}
}
