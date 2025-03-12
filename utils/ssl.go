package utils

import "errors"

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
