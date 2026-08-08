package driver

import (
	"crypto/tls"
	"net/url"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils/testutils"
)

func TestConfigURI_StripsTLSFileParamsWhenInlinePEMConfigured(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	cfg := &Config{
		Hosts:     []string{"mongo.example.com:27017"},
		AuthDB:    "admin",
		Username:  "user",
		Password:  "pass",
		TLSCACert: certs.CACert,
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

func TestConfigURI_PreservesTLSFileParamsWithoutInlinePEM(t *testing.T) {
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

func TestConfigURI_AddsTLSWhenInlinePEMConfigured(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	cfg := &Config{
		Hosts:     []string{"mongo.example.com:27017"},
		AuthDB:    "admin",
		Username:  "user",
		Password:  "pass",
		TLSCACert: certs.CACert,
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
	clientBundle := certs.ClientCert + certs.ClientKey

	tests := []struct {
		name       string
		config     *Config
		wantNil    bool
		assertions func(t *testing.T, tlsCfg *tls.Config)
	}{
		{
			name: "no inline tls returns nil",
			config: &Config{
				Hosts: []string{"mongo.example.com:27017"},
			},
			wantNil: true,
		},
		{
			name: "ca cert only uses verify-ca semantics",
			config: &Config{
				Hosts:     []string{"mongo.example.com:27017"},
				TLSCACert: certs.CACert,
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
			name: "client cert bundle enables mTLS",
			config: &Config{
				Hosts:             []string{"mongo.example.com:27017"},
				TLSCACert:         certs.CACert,
				TLSCertificateKey: clientBundle,
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

func TestSplitCertificateKeyPEM(t *testing.T) {
	certs := testutils.GenerateTestCerts()
	bundle := certs.ClientCert + certs.ClientKey

	certPEM, keyPEM, err := splitCertificateKeyPEM(bundle)
	if err != nil {
		t.Fatalf("splitCertificateKeyPEM() error = %v", err)
	}
	if !strings.Contains(certPEM, "BEGIN CERTIFICATE") {
		t.Fatalf("expected certificate PEM, got %q", certPEM)
	}
	if !strings.Contains(keyPEM, "BEGIN RSA PRIVATE KEY") {
		t.Fatalf("expected private key PEM, got %q", keyPEM)
	}
}

func TestConfigValidate_RejectsInvalidTLSPEM(t *testing.T) {
	cfg := &Config{
		Hosts:     []string{"mongo.example.com:27017"},
		TLSCACert: "not-a-pem-block",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for invalid tls_ca_cert")
	}
}
