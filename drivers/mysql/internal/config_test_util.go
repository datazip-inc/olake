package driver

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"sync"
	"time"
)

type testCerts struct {
	CACert     string
	ClientCert string
	ClientKey  string
}

var (
	generatedCerts *testCerts
	certOnce       sync.Once
)

// generateTestCerts creates self-signed test certificates for SSL testing
func generateTestCerts() *testCerts {
	certOnce.Do(func() {
		caKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic("failed to generate CA key: " + err.Error())
		}

		// CA certificate template
		caTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				Country:            []string{"IN"},
				Organization:       []string{"Olake Test"},
				OrganizationalUnit: []string{"Testing"},
				CommonName:         "Test CA",
			},
			NotBefore:             time.Now().Add(-1 * time.Hour),
			NotAfter:              time.Now().Add(24 * time.Hour),
			IsCA:                  true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
			BasicConstraintsValid: true,
		}

		caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
		if err != nil {
			panic("failed to create CA cert: " + err.Error())
		}

		caCertPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: caCertDER,
		})

		// Client certificate for mutual TLS
		clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic("failed to generate client key: " + err.Error())
		}

		clientTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject: pkix.Name{
				Country:            []string{"IN"},
				Organization:       []string{"Olake Test"},
				OrganizationalUnit: []string{"Testing"},
				CommonName:         "mysql",
			},
			NotBefore:   time.Now().Add(-1 * time.Hour),
			NotAfter:    time.Now().Add(24 * time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}

		clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
		if err != nil {
			panic("failed to create client cert: " + err.Error())
		}

		clientCertPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: clientCertDER,
		})

		clientKeyPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(clientKey),
		})

		generatedCerts = &testCerts{
			CACert:     string(caCertPEM),
			ClientCert: string(clientCertPEM),
			ClientKey:  string(clientKeyPEM),
		}
	})

	return generatedCerts
}
