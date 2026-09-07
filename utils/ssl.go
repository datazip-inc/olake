package utils

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	SSLModeRequire    = "require"
	SSLModeDisable    = "disable"
	SSLModeVerifyCA   = "verify-ca"
	SSLModeVerifyFull = "verify-full"

	Unknown = ""
)

type SSLField string

const (
	SSLFieldServerCA   SSLField = "ssl.server_ca"
	SSLFieldClientCert SSLField = "ssl.client_cert"
	SSLFieldClientKey  SSLField = "ssl.client_key"
)

// SSLConfig is a dto for deserialized SSL configuration for Postgres
type SSLConfig struct {
	Mode       string `mapstructure:"mode,omitempty" json:"mode,omitempty" yaml:"mode,omitempty"`
	ServerCA   string `mapstructure:"server_ca,omitempty" json:"server_ca,omitempty" yaml:"server_ca,omitempty"`
	ClientCert string `mapstructure:"client_cert,omitempty" json:"client_cert,omitempty" yaml:"client_cert,omitempty"`
	ClientKey  string `mapstructure:"client_key,omitempty" json:"client_key,omitempty" yaml:"client_key,omitempty"`
}

// codeSSLConfigInvalid marks an ssl block the user filled in wrong; no server was reached.
// One code per condition: two checks a user would fix differently never share one.
const (
	// The ssl block itself is missing a field.
	codeSSLMissing         = "config.ssl_missing"
	codeSSLModeMissing     = "config.ssl_mode_missing"
	codeSSLServerCAMissing = "config.ssl_server_ca_missing"

	// The PEM material the user supplied is unusable; nothing was sent to a server yet.
	codeSSLMaterialMissing        = "config.ssl_material_missing"
	codeSSLPEMNotCertificate      = "config.ssl_pem_not_certificate"
	codeSSLCertificateUnparseable = "config.ssl_certificate_unparseable"
	codeSSLPEMMalformed           = "config.ssl_pem_malformed"
	codeSSLPEMTrailingData        = "config.ssl_pem_trailing_data"
	codeSSLCAUnusable             = "config.ssl_ca_unusable"
	codeSSLClientKeypairInvalid   = "config.ssl_client_keypair_invalid"

	// Raised during the handshake, against the CA the user supplied.
	codeServerCertAbsent      = "tls.server_cert_absent"
	codeServerCertUnparseable = "tls.server_cert_unparseable"
	codeServerCertUnverified  = "tls.server_cert_unverified"
)

// Validate returns err if the ssl configuration is invalid
func (sc *SSLConfig) Validate() error {
	// TODO: Add Proper validations and test
	if sc == nil {
		return errs.Precondition(errs.ConfigInvalid, codeSSLMissing,
			errors.New("'ssl' config is required"))
	}

	if sc.Mode == Unknown {
		return errs.Precondition(errs.ConfigInvalid, codeSSLModeMissing,
			errors.New("'ssl.mode' is required parameter"))
	}

	if sc.Mode == SSLModeVerifyCA || sc.Mode == SSLModeVerifyFull {
		if sc.ServerCA == "" {
			return errs.Precondition(errs.ConfigInvalid, codeSSLServerCAMissing,
				errors.New("'ssl.server_ca' is required parameter"))
		}
	}

	return nil
}

// BuildTLSConfig returns a TLS config based on OLake SSL mode semantics.
func BuildTLSConfig(host string, sc *SSLConfig) (*tls.Config, error) {
	if sc == nil || sc.Mode == SSLModeDisable {
		// ssl is disabled, return nil (intentional nilnil)
		return nil, nil //nolint:nilnil
	}

	// For 'require' mode: encrypt connection but skip server identity verification.
	// #nosec G402 -- required by SSL mode semantics
	if sc.Mode == SSLModeRequire {
		return &tls.Config{
			InsecureSkipVerify: true, // #nosec G402
			MinVersion:         tls.VersionTLS12,
		}, nil
	}

	rootCertPool := x509.NewCertPool()
	serverCAPEM, err := readPEMData(sc.ServerCA, SSLFieldServerCA, true)
	if err != nil {
		return nil, err
	}
	if ok := rootCertPool.AppendCertsFromPEM(serverCAPEM); !ok {
		return nil, errs.Precondition(errs.ConfigInvalid, codeSSLCAUnusable,
			fmt.Errorf("failed to append CA certificate"))
	}

	tlsConfig := &tls.Config{
		RootCAs:    rootCertPool,
		MinVersion: tls.VersionTLS12,
	}

	if sc.Mode == SSLModeVerifyCA {
		// verify-ca validates cert chain but skips hostname verification.
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errs.Precondition(errs.TLSFailed, codeServerCertAbsent,
					fmt.Errorf("no server certificate provided"))
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return errs.Precondition(errs.TLSFailed, codeServerCertUnparseable,
					fmt.Errorf("failed to parse server certificate: %w", err))
			}

			intermediates := x509.NewCertPool()
			for i := 1; i < len(rawCerts); i++ {
				intermediateCert, err := x509.ParseCertificate(rawCerts[i])
				if err != nil {
					logger.Warnf("failed to parse intermediate certificate at position %d: %s", i, err)
					continue
				}
				intermediates.AddCert(intermediateCert)
			}

			verifyOpts := x509.VerifyOptions{
				Roots:         rootCertPool,
				Intermediates: intermediates,
			}
			if _, err := cert.Verify(verifyOpts); err != nil {
				return errs.Precondition(errs.TLSFailed, codeServerCertUnverified,
					fmt.Errorf("failed to verify server certificate against CA: %w", err))
			}
			return nil
		}
	} else {
		// verify-full validates both cert chain and hostname.
		tlsConfig.ServerName = host
	}

	if sc.ClientCert != "" && sc.ClientKey != "" {
		clientCertPEM, err := readPEMData(sc.ClientCert, SSLFieldClientCert, true)
		if err != nil {
			return nil, err
		}
		clientKeyPEM, err := readPEMData(sc.ClientKey, SSLFieldClientKey, false)
		if err != nil {
			return nil, err
		}
		clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		if err != nil {
			return nil, errs.Precondition(errs.ConfigInvalid, codeSSLClientKeypairInvalid,
				fmt.Errorf("failed to load client certificate and key: %s", err))
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	return tlsConfig, nil
}

func readPEMData(value string, field SSLField, parseAsCert bool) ([]byte, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, errs.Precondition(errs.ConfigInvalid, codeSSLMaterialMissing,
			fmt.Errorf("'%s' is required", string(field)))
	}

	// PEM files may contain multiple blocks (e.g., certificate chains).
	remaining := []byte(trimmed)
	foundBlock := false

	for {
		var block *pem.Block
		block, remaining = pem.Decode(remaining)
		if block == nil {
			break
		}
		foundBlock = true

		if parseAsCert {
			if block.Type != "CERTIFICATE" {
				return nil, errs.Precondition(errs.ConfigInvalid, codeSSLPEMNotCertificate,
					fmt.Errorf("'%s' must contain CERTIFICATE PEM blocks", string(field)))
			}
			if _, err := x509.ParseCertificate(block.Bytes); err != nil {
				return nil, errs.Precondition(errs.ConfigInvalid, codeSSLCertificateUnparseable,
					fmt.Errorf("'%s' contains an invalid certificate: %w", string(field), err))
			}
		}
	}

	if !foundBlock {
		return nil, errs.Precondition(errs.ConfigInvalid, codeSSLPEMMalformed,
			fmt.Errorf("'%s' is not a valid PEM encoded block", string(field)))
	}
	if strings.TrimSpace(string(remaining)) != "" {
		return nil, errs.Precondition(errs.ConfigInvalid, codeSSLPEMTrailingData,
			fmt.Errorf("'%s' must contain only PEM blocks", string(field)))
	}

	return []byte(trimmed), nil
}
