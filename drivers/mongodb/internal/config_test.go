package driver

import (
	"crypto/tls"
	"net/url"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestConfigURI_StripsTLSFileParamsWhenSSLEnabled(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	cfg := &Config{
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
	}

	parsed, err := url.Parse(cfg.URI())
	if err != nil {
		t.Fatalf("parse uri: %v", err)
	}

	query := parsed.Query()
	if query.Get("tlsCAFile") != "" {
		t.Fatalf("expected tlsCAFile to be stripped, got %q", query.Get("tlsCAFile"))
	}
	if query.Get("tlsCertificateKeyFile") != "" {
		t.Fatalf("expected tlsCertificateKeyFile to be stripped, got %q", query.Get("tlsCertificateKeyFile"))
	}
	if query.Get("tls") != "true" {
		t.Fatalf("expected tls=true, got %q", query.Get("tls"))
	}
	if query.Get("connectTimeoutMS") != "5000" {
		t.Fatalf("expected connectTimeoutMS to be preserved, got %q", query.Get("connectTimeoutMS"))
	}
}

func TestConfigURI_PreservesTLSFileParamsWithoutSSL(t *testing.T) {
	cfg := &Config{
		Hosts:    []string{"mongo.example.com:27017"},
		AuthDB:   "admin",
		Username: "user",
		Password: "pass",
		AdditionalParams: map[string]string{
			"tls":       "true",
			"tlsCAFile": "/certs/root-ca.crt",
		},
	}

	query, err := url.Parse(cfg.URI())
	if err != nil {
		t.Fatalf("parse uri: %v", err)
	}

	if query.Query().Get("tlsCAFile") != "/certs/root-ca.crt" {
		t.Fatalf("expected tlsCAFile to be preserved for CLI usage, got %q", query.Query().Get("tlsCAFile"))
	}
}

func TestConfigURI_AddsTLSWhenSSLEnabled(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	cfg := &Config{
		Hosts:    []string{"mongo.example.com:27017"},
		AuthDB:   "admin",
		Username: "user",
		Password: "pass",
		SSLConfiguration: &utils.SSLConfig{
			Mode:     utils.SSLModeVerifyCA,
			ServerCA: certs.CACert,
		},
	}

	query, err := url.Parse(cfg.URI())
	if err != nil {
		t.Fatalf("parse uri: %v", err)
	}

	if query.Query().Get("tls") != "true" {
		t.Fatalf("expected tls=true to be added automatically, got %q", query.Query().Get("tls"))
	}
}

func TestConfigBuildTLSConfig(t *testing.T) {
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

func TestConfigValidate_RejectsInvalidSSLPEM(t *testing.T) {
	cfg := &Config{
		Hosts:    []string{"mongo.example.com:27017"},
		Database: "testdb",
		Username: "user",
		Password: "pass",
		AuthDB:   "admin",
		SSLConfiguration: &utils.SSLConfig{
			Mode:     utils.SSLModeVerifyCA,
			ServerCA: "not-a-pem-block",
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for invalid ssl.server_ca")
	}
}

func TestConfigValidate_RequiresServerCAForVerifyCA(t *testing.T) {
	cfg := &Config{
		Hosts:    []string{"mongo.example.com:27017"},
		Database: "testdb",
		Username: "user",
		Password: "pass",
		AuthDB:   "admin",
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeVerifyCA,
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for missing ssl.server_ca")
	}
}

func TestConfigValidate_RequiresHostsDatabaseAndAuth(t *testing.T) {
	cfg := &Config{
		Hosts:    []string{},
		Database: "",
		Username: "",
		Password: "",
		AuthDB:   "",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for missing required fields")
	}
}

func TestConfigValidate_SetsDefaults(t *testing.T) {
	cfg := &Config{
		Hosts:    []string{"mongo.example.com:27017"},
		Database: "testdb",
		Username: "user",
		Password: "pass",
		AuthDB:   "admin",
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.MaxThreads == 0 {
		t.Fatalf("expected MaxThreads default to be set")
	}
	if cfg.RetryCount == 0 {
		t.Fatalf("expected RetryCount default to be set")
	}
	if cfg.SSLConfiguration == nil || cfg.SSLConfiguration.Mode != utils.SSLModeDisable {
		t.Fatalf("expected default ssl mode disable, got %#v", cfg.SSLConfiguration)
	}
}

func TestConfigBuildTLSConfig_VerifyFullLeavesServerNameEmpty(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name  string
		hosts []string
	}{
		{
			name:  "single host",
			hosts: []string{"mongo1.internal:27017"},
		},
		{
			name: "multi host replica set",
			hosts: []string{
				"mongo1.internal:27017",
				"mongo2.internal:27017",
				"mongo3.internal:27017",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Hosts: tt.hosts,
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyFull,
					ServerCA: certs.CACert,
				},
			}

			tlsCfg, err := cfg.buildTLSConfig()
			if err != nil {
				t.Fatalf("buildTLSConfig() error = %v", err)
			}
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
		})
	}
}
