package driver

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestConfig_URI(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name        string
		config      *Config
		wantURI     string
		notContains []string
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
			wantURI: "mongodb://user:pass@mongo.example.com:27017/?authSource=admin&connectTimeoutMS=5000&tls=true",
			notContains: []string{
				"tlsCAFile",
				"tlsCertificateKeyFile",
			},
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
			wantURI: "mongodb://user:pass@mongo.example.com:27017/?authSource=admin&tls=true&tlsCAFile=%2Fcerts%2Froot-ca.crt",
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
			wantURI: "mongodb://user:pass@mongo.example.com:27017/?authSource=admin&tls=true",
		},
		{
			name: "x509 client cert auth has no password",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				AuthDB:   "$external",
				Username: "CN=olake-client,OU=Data,O=Acme",
				AdditionalParams: map[string]string{
					"authMechanism": AuthMechanismX509,
					"tls":           "true",
				},
			},
			wantURI: "mongodb://CN=olake-client,OU=Data,O=Acme@mongo.internal:27017/?authMechanism=MONGODB-X509&authSource=%24external&tls=true",
		},
		{
			name: "username without password",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				AuthDB:   "admin",
				Username: "appuser",
			},
			wantURI: "mongodb://appuser@mongo.internal:27017/?authSource=admin",
		},
		{
			name: "MONGODB-OIDC without password via additional_params",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				AuthDB:   "$external",
				Username: "olake-oidc-client",
				AdditionalParams: map[string]string{
					"authMechanism": AuthMechanismOIDC,
				},
			},
			wantURI: "mongodb://olake-oidc-client@mongo.internal:27017/?authMechanism=MONGODB-OIDC&authSource=%24external",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.URI()
			if got != tt.wantURI {
				t.Fatalf("URI() = %q, want %q", got, tt.wantURI)
			}
			for _, s := range tt.notContains {
				if strings.Contains(got, s) {
					t.Fatalf("URI() = %q, must not contain %q", got, s)
				}
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name           string
		config         *Config
		expectErr      bool
		wantMaxThreads int
		wantRetryCount int
		wantSSLMode    string
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
			name: "negative max threads fails validation",
			config: &Config{
				Hosts:      []string{"mongo.example.com:27017"},
				Database:   "testdb",
				Username:   "user",
				Password:   "pass",
				AuthDB:     "admin",
				MaxThreads: -1,
			},
			expectErr: true,
		},
		{
			name: "negative retry count fails validation",
			config: &Config{
				Hosts:      []string{"mongo.example.com:27017"},
				Database:   "testdb",
				Username:   "user",
				Password:   "pass",
				AuthDB:     "admin",
				RetryCount: -1,
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
			expectErr:      false,
			wantMaxThreads: constants.DefaultThreadCount,
			wantRetryCount: constants.DefaultRetryCount,
			wantSSLMode:    utils.SSLModeDisable,
		},
		{
			name: "x509 without password passes validate",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Database: "analytics",
				AuthDB:   "$external",
				Username: "CN=olake-client,OU=Data,O=Acme",
				AdditionalParams: map[string]string{
					"authMechanism": AuthMechanismX509,
					"tls":           "true",
				},
			},
			expectErr: false,
		},
		{
			name: "username without password passes validate",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Database: "analytics",
				AuthDB:   "admin",
				Username: "appuser",
			},
			expectErr: false,
		},
		{
			name: "MONGODB-OIDC without password passes validate",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Database: "analytics",
				AuthDB:   "$external",
				Username: "olake-oidc-client",
				AdditionalParams: map[string]string{
					"authMechanism": AuthMechanismOIDC,
				},
			},
			expectErr: false,
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
			if tt.wantMaxThreads != 0 && tt.config.MaxThreads != tt.wantMaxThreads {
				t.Fatalf("MaxThreads = %d, want %d", tt.config.MaxThreads, tt.wantMaxThreads)
			}
			if tt.wantRetryCount != 0 && tt.config.RetryCount != tt.wantRetryCount {
				t.Fatalf("RetryCount = %d, want %d", tt.config.RetryCount, tt.wantRetryCount)
			}
			if tt.wantSSLMode != "" {
				if tt.config.SSLConfiguration == nil {
					t.Fatalf("SSLConfiguration is nil, want mode %q", tt.wantSSLMode)
				}
				if tt.config.SSLConfiguration.Mode != tt.wantSSLMode {
					t.Fatalf("SSL mode = %q, want %q", tt.config.SSLConfiguration.Mode, tt.wantSSLMode)
				}
			}
		})
	}
}

type wantTLS struct {
	nilConfig          bool
	insecureSkipVerify bool
	serverName         string
	minVersion         uint16
	hasRootCAs         bool
	hasVerifyPeerCert  bool
	certCount          int
}

func TestConfig_buildTLSConfig(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		want    wantTLS
	}{
		{
			name: "no ssl returns nil",
			config: &Config{
				Hosts: []string{"mongo.example.com:27017"},
			},
			want: wantTLS{nilConfig: true},
		},
		{
			name: "ssl disabled returns nil",
			config: &Config{
				Hosts: []string{"mongo.example.com:27017"},
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeDisable,
				},
			},
			want: wantTLS{nilConfig: true},
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
			want: wantTLS{
				insecureSkipVerify: true,
				minVersion:         tls.VersionTLS12,
				hasRootCAs:         true,
				hasVerifyPeerCert:  true,
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
			want: wantTLS{
				insecureSkipVerify: true,
				minVersion:         tls.VersionTLS12,
				hasRootCAs:         true,
				hasVerifyPeerCert:  true,
				certCount:          1,
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
			want: wantTLS{
				serverName:        "",
				minVersion:        tls.VersionTLS12,
				hasRootCAs:        true,
				hasVerifyPeerCert: false,
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
			want: wantTLS{
				serverName:        "",
				minVersion:        tls.VersionTLS12,
				hasRootCAs:        true,
				hasVerifyPeerCert: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.config.buildTLSConfig()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("buildTLSConfig() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("buildTLSConfig() error = %v", err)
			}
			if tt.want.nilConfig {
				if got != nil {
					t.Fatalf("expected nil tls config, got %#v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected tls config, got nil")
			}
			if got.InsecureSkipVerify != tt.want.insecureSkipVerify {
				t.Fatalf("InsecureSkipVerify = %v, want %v", got.InsecureSkipVerify, tt.want.insecureSkipVerify)
			}
			if got.ServerName != tt.want.serverName {
				t.Fatalf("ServerName = %q, want %q", got.ServerName, tt.want.serverName)
			}
			if tt.want.minVersion != 0 && got.MinVersion != tt.want.minVersion {
				t.Fatalf("MinVersion = %#x, want %#x", got.MinVersion, tt.want.minVersion)
			}
			if tt.want.hasRootCAs && got.RootCAs == nil {
				t.Fatalf("expected root CAs to be configured")
			}
			if !tt.want.hasRootCAs && got.RootCAs != nil {
				t.Fatalf("expected no root CAs, got %#v", got.RootCAs)
			}
			if tt.want.hasVerifyPeerCert && got.VerifyPeerCertificate == nil {
				t.Fatalf("expected custom peer certificate verification")
			}
			if !tt.want.hasVerifyPeerCert && got.VerifyPeerCertificate != nil {
				t.Fatalf("expected no custom VerifyPeerCertificate")
			}
			if len(got.Certificates) != tt.want.certCount {
				t.Fatalf("len(Certificates) = %d, want %d", len(got.Certificates), tt.want.certCount)
			}
		})
	}
}
