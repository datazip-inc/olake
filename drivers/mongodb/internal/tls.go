package driver

import (
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/utils"
)

var mongoTLSFileParamKeys = []string{"tlsCAFile", "tlsCertificateKeyFile"}

func (c *Config) hasInlineTLS() bool {
	return strings.TrimSpace(c.TLSCACert) != "" || strings.TrimSpace(c.TLSCertificateKey) != ""
}

func (c *Config) uriAdditionalParams() map[string]string {
	if !c.hasInlineTLS() {
		return c.AdditionalParams
	}

	params := make(map[string]string, len(c.AdditionalParams)+1)
	for key, value := range c.AdditionalParams {
		if isMongoTLSFileParam(key) {
			continue
		}
		params[key] = value
	}

	if _, ok := params["tls"]; !ok {
		params["tls"] = "true"
	}

	return params
}

func isMongoTLSFileParam(key string) bool {
	for _, blocked := range mongoTLSFileParamKeys {
		if key == blocked {
			return true
		}
	}
	return false
}

func mongoTLSHost(hosts []string) string {
	if len(hosts) == 0 {
		return ""
	}

	host := hosts[0]
	if h, _, ok := strings.Cut(host, ":"); ok {
		return h
	}
	return host
}

func (c *Config) buildTLSConfig() (*tls.Config, error) {
	caPEM := strings.TrimSpace(c.TLSCACert)
	certKeyPEM := strings.TrimSpace(c.TLSCertificateKey)

	if caPEM == "" && certKeyPEM == "" {
		return nil, nil
	}

	sslCfg := &utils.SSLConfig{}
	switch {
	case caPEM != "":
		sslCfg.Mode = utils.SSLModeVerifyCA
		sslCfg.ServerCA = caPEM
	default:
		sslCfg.Mode = utils.SSLModeRequire
	}

	if certKeyPEM != "" {
		clientCert, clientKey, err := splitCertificateKeyPEM(certKeyPEM)
		if err != nil {
			return nil, err
		}
		sslCfg.ClientCert = clientCert
		sslCfg.ClientKey = clientKey
	}

	if err := sslCfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate tls config: %w", err)
	}

	return utils.BuildTLSConfig(mongoTLSHost(c.Hosts), sslCfg)
}

func splitCertificateKeyPEM(bundle string) (string, string, error) {
	trimmed := strings.TrimSpace(bundle)
	if trimmed == "" {
		return "", "", fmt.Errorf("tls_certificate_key is required")
	}

	var certBlocks, keyBlocks []string
	remaining := []byte(trimmed)

	for {
		block, rest := pem.Decode(remaining)
		if block == nil {
			break
		}

		encoded := string(pem.EncodeToMemory(block))
		switch block.Type {
		case "CERTIFICATE":
			certBlocks = append(certBlocks, encoded)
		case "RSA PRIVATE KEY", "EC PRIVATE KEY", "PRIVATE KEY":
			keyBlocks = append(keyBlocks, encoded)
		default:
			return "", "", fmt.Errorf("tls_certificate_key contains unsupported PEM block type %q", block.Type)
		}
		remaining = rest
	}

	if len(certBlocks) == 0 {
		return "", "", fmt.Errorf("tls_certificate_key must contain at least one CERTIFICATE PEM block")
	}
	if len(keyBlocks) == 0 {
		return "", "", fmt.Errorf("tls_certificate_key must contain a private key PEM block")
	}

	return strings.Join(certBlocks, ""), strings.Join(keyBlocks, ""), nil
}
