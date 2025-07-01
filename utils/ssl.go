package utils

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
)

const (
	SSLModeRequire    = "require"
	SSLModeDisable    = "disable"
	SSLModeVerifyCA   = "verify-ca"
	SSLModeVerifyFull = "verify-full"

	Unknown = ""
)

// SSLConfig is a dto for deserialized SSL configuration for Postgres
type SSLConfig struct {
	// SSL mode
	//
	// @jsonschema(
	// required=true,
	// enum=["require","disable","verify-ca","verify-full"]
	// )
	Mode string `mapstructure:"mode,omitempty" json:"mode,omitempty" yaml:"mode,omitempty"`
	// CA Certificate
	//
	// @jsonschema(
	// title="CA Certificate"
	// )
	ServerCA string `mapstructure:"server_ca,omitempty" json:"server_ca,omitempty" yaml:"server_ca,omitempty"`
	// Client Certificate
	//
	// @jsonschema(
	// title="Client Certificate"
	// )
	ClientCert string `mapstructure:"client_cert,omitempty" json:"client_cert,omitempty" yaml:"client_cert,omitempty"`
	// Client Certificate Key
	//
	// @jsonschema(
	// title="Client Certificate Key"
	// )
	ClientKey string `mapstructure:"client_key,omitempty" json:"client_key,omitempty" yaml:"client_key,omitempty"`
}

// Validate returns err if the ssl configuration is invalid
func (sc *SSLConfig) Validate() error {
	// TODO: Add Proper validations and test
	if sc == nil {
		return errors.New("'ssl' config is required")
	}

	if sc.Mode == Unknown {
		return errors.New("'ssl.mode' is required parameter")
	}

	if sc.Mode == SSLModeVerifyCA || sc.Mode == SSLModeVerifyFull {
		if sc.ServerCA == "" {
			return errors.New("'ssl.server_ca' is required parameter")
		}

		if sc.ClientCert == "" {
			return errors.New("'ssl.client_cert' is required parameter")
		}

		if sc.ClientKey == "" {
			return errors.New("'ssl.client_key' is required parameter")
		}
	}

	return nil
}

// CreateTLSConfiguration creates a TLS configuration from the SSLConfig
func CreateTLSConfiguration(sslConfig *SSLConfig) (*tls.Config, error) {
	if sslConfig == nil {
		return nil, errors.New("SSL configuration is nil")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// For verify-ca and verify-full modes, we need to set up certificates
	if sslConfig.Mode == SSLModeVerifyCA || sslConfig.Mode == SSLModeVerifyFull {
		// Create a certificate pool and add the server CA certificate
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM([]byte(sslConfig.ServerCA)); !ok {
			return nil, errors.New("failed to append server CA certificate")
		}
		tlsConfig.RootCAs = caCertPool

		// If client certificates are provided, load them
		if sslConfig.ClientCert != "" && sslConfig.ClientKey != "" {
			cert, err := tls.X509KeyPair([]byte(sslConfig.ClientCert), []byte(sslConfig.ClientKey))
			if err != nil {
				return nil, errors.New("failed to load client certificate and key: " + err.Error())
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// For verify-full, we need to verify the server name
		if sslConfig.Mode == SSLModeVerifyFull {
			tlsConfig.InsecureSkipVerify = false
		} else {
			// For verify-ca, we don't need to verify the server name
			tlsConfig.InsecureSkipVerify = true
		}
	} else if sslConfig.Mode == SSLModeRequire {
		// For require mode, we don't verify the server certificate
		tlsConfig.InsecureSkipVerify = true
	}

	return tlsConfig, nil
}
