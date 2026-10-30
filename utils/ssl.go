package utils

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	SSLModeRequire    = "require"
	SSLModeDisable    = "disable"
	SSLModeVerifyCA   = "verify-ca"
	SSLModeVerifyFull = "verify-full"

	SSLCertSourceInline = "inline"
	SSLCertSourcePath   = "path"

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

	// CertSource is "inline" (default) to use the PEM content fields above, or "path"
	// to read them from the files at ServerCAPath, ClientCertPath and ClientKeyPath.
	CertSource     string `mapstructure:"cert_source,omitempty" json:"cert_source,omitempty" yaml:"cert_source,omitempty"`
	ServerCAPath   string `mapstructure:"server_ca_path,omitempty" json:"server_ca_path,omitempty" yaml:"server_ca_path,omitempty"`
	ClientCertPath string `mapstructure:"client_cert_path,omitempty" json:"client_cert_path,omitempty" yaml:"client_cert_path,omitempty"`
	ClientKeyPath  string `mapstructure:"client_key_path,omitempty" json:"client_key_path,omitempty" yaml:"client_key_path,omitempty"`
}

// ValidateCertSource rejects any CertSource other than "", "inline" or "path".
// Callers must run it before LoadCertFiles.
func (sc *SSLConfig) ValidateCertSource() error {
	if sc == nil {
		return nil
	}
	switch sc.CertSource {
	case Unknown, SSLCertSourceInline, SSLCertSourcePath:
		return nil
	}
	return errs.Precondition(errs.ConfigInvalid, codeSSLCertSourceInvalid,
		fmt.Errorf("'ssl.cert_source' must be '%s' or '%s', got '%s'", SSLCertSourceInline, SSLCertSourcePath, sc.CertSource))
}

// LoadCertFiles reads the files at ServerCAPath, ClientCertPath and ClientKeyPath into
// ServerCA, ClientCert and ClientKey when CertSource is "path". It is a no-op otherwise,
// so all later code works on PEM content only. It does not validate CertSource; call
// ValidateCertSource first.
func (sc *SSLConfig) LoadCertFiles() error {
	if sc == nil || sc.CertSource != SSLCertSourcePath {
		return nil
	}
	for _, cert := range []struct {
		filePath  string  // file to read
		pemField  *string // receives the PEM content read from filePath
		configKey string  // path config key named in errors
		inlineKey string  // inline config key replaced by the file content
	}{
		{sc.ServerCAPath, &sc.ServerCA, "ssl.server_ca_path", "server_ca"},
		{sc.ClientCertPath, &sc.ClientCert, "ssl.client_cert_path", "client_cert"},
		{sc.ClientKeyPath, &sc.ClientKey, "ssl.client_key_path", "client_key"},
	} {
		if strings.TrimSpace(cert.filePath) == "" {
			continue
		}
		data, err := os.ReadFile(strings.TrimSpace(cert.filePath))
		if err != nil {
			return errs.Precondition(errs.ConfigInvalid, codeSSLFileUnreadable,
				fmt.Errorf("'%s': failed to read file: %w", cert.configKey, err))
		}

		if inline := strings.TrimSpace(*cert.pemField); inline != "" && inline != strings.TrimSpace(string(data)) {
			return errs.Precondition(errs.ConfigInvalid, codeSSLCertSourceConflict,
				fmt.Errorf("'ssl.%s' and '%s' are both set while ssl.cert_source is 'path'; remove 'ssl.%s'", cert.inlineKey, cert.configKey, cert.inlineKey))
		}
		*cert.pemField = string(data)
	}
	return nil
}

const (
	// The ssl block itself is missing a field.
	codeSSLMissing            = "config.ssl_missing"
	codeSSLModeMissing        = "config.ssl_mode_missing"
	codeSSLServerCAMissing    = "config.ssl_server_ca_missing"
	codeSSLCertSourceInvalid  = "config.ssl_cert_source_invalid"
	codeSSLCertSourceConflict = "config.ssl_cert_source_conflict"

	// Certificate file could not be read from disk.
	codeSSLFileUnreadable = "config.ssl_file_unreadable"

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

	if err := sc.ValidateCertSource(); err != nil {
		return err
	}

	// Cert files are read only for modes that consume them; a stale path must not
	// reject a config whose mode ignores the certificates.
	if sc.Mode == SSLModeVerifyCA || sc.Mode == SSLModeVerifyFull {
		if err := sc.LoadCertFiles(); err != nil {
			return err
		}
		if sc.ServerCA == "" {
			return errs.Precondition(errs.ConfigInvalid, codeSSLServerCAMissing,
				errors.New("server CA certificate is required: provide 'ssl.server_ca', or set 'ssl.cert_source' to 'path' with a non-empty file at 'ssl.server_ca_path'"))
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
