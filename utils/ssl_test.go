package utils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var serial int64

// issue signs template with parent, or self-signs it when parent is nil.
func issue(t *testing.T, template, parent *x509.Certificate, parentKey *rsa.PrivateKey) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	if parent == nil {
		parent, parentKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

func caTemplate(commonName string) *x509.Certificate {
	serial++
	return &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
}

func leafTemplate(commonName string) *x509.Certificate {
	serial++
	return &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: commonName},
		DNSNames:     []string{commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
}

func certPEM(cert *x509.Certificate) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
}

func TestBuildTLSConfig(t *testing.T) {
	ca, caKey := issue(t, caTemplate("root"), nil, nil)
	caPEM := certPEM(ca)

	t.Run("nil config disables tls", func(t *testing.T) {
		cfg, err := BuildTLSConfig("db.internal", nil)
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("disable mode disables tls", func(t *testing.T) {
		cfg, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeDisable})
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("require encrypts without verifying", func(t *testing.T) {
		cfg, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeRequire})
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.True(t, cfg.InsecureSkipVerify)
		assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
		assert.Nil(t, cfg.RootCAs)
		assert.Nil(t, cfg.VerifyConnection)
	})

	t.Run("verify-ca checks the chain but not the hostname", func(t *testing.T) {
		cfg, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeVerifyCA, ServerCA: caPEM})
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.True(t, cfg.InsecureSkipVerify)
		assert.Empty(t, cfg.ServerName)
		assert.NotNil(t, cfg.VerifyConnection)
	})

	t.Run("verify-full checks the hostname", func(t *testing.T) {
		cfg, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeVerifyFull, ServerCA: caPEM})
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.False(t, cfg.InsecureSkipVerify)
		assert.Equal(t, "db.internal", cfg.ServerName)
		assert.Nil(t, cfg.VerifyConnection)
	})

	t.Run("missing server ca", func(t *testing.T) {
		_, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeVerifyCA})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "'ssl.server_ca' is required")
	})

	t.Run("malformed server ca", func(t *testing.T) {
		_, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeVerifyCA, ServerCA: "not a pem block"})
		require.Error(t, err)
	})

	t.Run("client certificate is loaded for mutual tls", func(t *testing.T) {
		clientCert, clientKey := issue(t, leafTemplate("olake-client"), ca, caKey)
		keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)}))

		cfg, err := BuildTLSConfig("db.internal", &SSLConfig{
			Mode:       SSLModeVerifyCA,
			ServerCA:   caPEM,
			ClientCert: certPEM(clientCert),
			ClientKey:  keyPEM,
		})
		require.NoError(t, err)
		require.Len(t, cfg.Certificates, 1)
	})

	t.Run("client certificate without its key is ignored", func(t *testing.T) {
		clientCert, _ := issue(t, leafTemplate("olake-client"), ca, caKey)

		cfg, err := BuildTLSConfig("db.internal", &SSLConfig{
			Mode:       SSLModeVerifyCA,
			ServerCA:   caPEM,
			ClientCert: certPEM(clientCert),
		})
		require.NoError(t, err)
		assert.Empty(t, cfg.Certificates)
	})

	t.Run("malformed client certificate", func(t *testing.T) {
		_, err := BuildTLSConfig("db.internal", &SSLConfig{
			Mode:       SSLModeVerifyCA,
			ServerCA:   caPEM,
			ClientCert: "not a pem block",
			ClientKey:  "not a pem block either",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ssl.client_cert")
	})

	t.Run("malformed client key", func(t *testing.T) {
		clientCert, _ := issue(t, leafTemplate("olake-client"), ca, caKey)

		_, err := BuildTLSConfig("db.internal", &SSLConfig{
			Mode:       SSLModeVerifyCA,
			ServerCA:   caPEM,
			ClientCert: certPEM(clientCert),
			ClientKey:  "not a pem block",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ssl.client_key")
	})

	t.Run("client key belonging to another certificate", func(t *testing.T) {
		clientCert, _ := issue(t, leafTemplate("olake-client"), ca, caKey)
		_, otherKey := issue(t, leafTemplate("other-client"), ca, caKey)
		keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(otherKey)}))

		_, err := BuildTLSConfig("db.internal", &SSLConfig{
			Mode:       SSLModeVerifyCA,
			ServerCA:   caPEM,
			ClientCert: certPEM(clientCert),
			ClientKey:  keyPEM,
		})
		require.Error(t, err)
	})

	t.Run("server ca that is not a certificate", func(t *testing.T) {
		keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("nonsense")}))

		_, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeVerifyCA, ServerCA: keyPEM})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must contain CERTIFICATE PEM blocks")
	})
}

// TestBuildTLSConfigVerifyCA drives the verify-ca callback with synthetic connection states, the
// closest a unit test gets to a handshake.
func TestBuildTLSConfigVerifyCA(t *testing.T) {
	ca, caKey := issue(t, caTemplate("root"), nil, nil)
	leaf, _ := issue(t, leafTemplate("db.internal"), ca, caKey)

	intermediate, intermediateKey := issue(t, caTemplate("intermediate"), ca, caKey)
	chainedLeaf, _ := issue(t, leafTemplate("db.internal"), intermediate, intermediateKey)

	otherCA, otherCAKey := issue(t, caTemplate("other root"), nil, nil)
	foreignLeaf, _ := issue(t, leafTemplate("db.internal"), otherCA, otherCAKey)

	cfg, err := BuildTLSConfig("db.internal", &SSLConfig{Mode: SSLModeVerifyCA, ServerCA: certPEM(ca)})
	require.NoError(t, err)
	require.NotNil(t, cfg.VerifyConnection)

	tests := []struct {
		name        string
		peers       []*x509.Certificate
		errContains string
	}{
		{
			name:  "leaf signed by the configured ca",
			peers: []*x509.Certificate{leaf},
		},
		{
			name:  "leaf reaching the ca through an intermediate",
			peers: []*x509.Certificate{chainedLeaf, intermediate},
		},
		{
			name:        "leaf from an unrelated ca",
			peers:       []*x509.Certificate{foreignLeaf},
			errContains: "failed to verify server certificate against CA",
		},
		{
			name:        "intermediate present but leaf from an unrelated ca",
			peers:       []*x509.Certificate{foreignLeaf, intermediate},
			errContains: "failed to verify server certificate against CA",
		},
		{
			name:        "no certificates offered",
			errContains: "no server certificate provided",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := cfg.VerifyConnection(tls.ConnectionState{PeerCertificates: tc.peers})
			if tc.errContains == "" {
				assert.NoError(t, err)
				return
			}
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), tc.errContains)
			}
		})
	}
}
