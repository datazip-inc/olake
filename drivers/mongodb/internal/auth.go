package driver

import "fmt"

type AuthMechanism string

const (
	AuthMechanismDefault     AuthMechanism = "" // SCRAM-SHA-1 (MongoDB default)
	AuthMechanismSCRAMSHA1   AuthMechanism = "SCRAM-SHA-1"
	AuthMechanismSCRAMSHA256 AuthMechanism = "SCRAM-SHA-256"
	AuthMechanismX509        AuthMechanism = "MONGODB-X509"
	AuthMechanismPLAINTEXT   AuthMechanism = "PLAIN"      // LDAP
	AuthMechanismGSSAPI      AuthMechanism = "GSSAPI"     // Kerberos
	AuthMechanismMONGOAWS    AuthMechanism = "MONGO-AWS"  // AWS IAM
	AuthMechanismMONGODBCR   AuthMechanism = "MONGODB-CR" // Legacy (deprecated)
)

type TLSConfig struct {
	Mode       string `json:"mode"`
	ServerCA   string `json:"server_ca"`
	ClientCert string `json:"client_cert"`
	ClientKey  string `json:"client_key"`
}

func (tc *TLSConfig) Validate() error {
	if tc.Mode == "" || tc.Mode == "disable" {
		return nil
	}
	if (tc.Mode == "verify-ca" || tc.Mode == "verify-full") && tc.ServerCA == "" {
		return fmt.Errorf("tls.server_ca is required for %s mode", tc.Mode)
	}
	if tc.ClientCert != "" && tc.ClientKey == "" {
		return fmt.Errorf("tls.client_key is required when tls.client_cert is provided")
	}
	if tc.ClientCert == "" && tc.ClientKey != "" {
		return fmt.Errorf("tls.client_cert is required when tls.client_key is provided")
	}
	return nil
}
