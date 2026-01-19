package driver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// tlsConfigMutex ensures thread-safe registration of TLS configurations
var tlsConfigMutex sync.Mutex

// Config represents the configuration for connecting to a MySQL database
type Config struct {
	Host             string            `json:"hosts"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	Database         string            `json:"database"`
	Port             int               `json:"port"`
	JDBCURLParams    map[string]string `json:"jdbc_url_params"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	UpdateMethod     interface{}       `json:"update_method"`
	MaxThreads       int               `json:"max_threads"`
	RetryCount       int               `json:"backoff_retry_count"`
	SSHConfig        *utils.SSHConfig  `json:"ssh_config"`
}

type CDC struct {
	InitialWaitTime int `json:"initial_wait_time"`
}

// URI generates the connection URI for the MySQL database
func (c *Config) URI() string {
	// Set default port if not specified
	if c.Port == 0 {
		c.Port = 3306
	}
	// Construct host string
	hostStr := c.Host
	if c.Host == "" {
		hostStr = "localhost"
	}

	cfg := mysql.Config{
		User:                 c.Username,
		Passwd:               c.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", hostStr, c.Port),
		DBName:               c.Database,
		AllowNativePasswords: true,
	}

	if c.SSLConfiguration != nil {
		switch c.SSLConfiguration.Mode {
		case utils.SSLModeDisable:
			cfg.TLSConfig = "false"
		case utils.SSLModeRequire:
			cfg.TLSConfig = "skip-verify"
		case utils.SSLModeVerifyCA, utils.SSLModeVerifyFull:
			// Note: verify-full (verify-identity) requires certificates with Subject Alternative Names (SAN).
			// Certificates using only the legacy Common Name (CN) field will be rejected.
			tlsConfig, err := c.buildTLSConfig()
			if err != nil {
				logger.Errorf("Failed to build TLS config: %v", err)
				cfg.Addr = "invalid-ssl-config:0"
				cfg.TLSConfig = "false"
			} else {
				// Unique TLS config name to avoid conflicts with multiple connections
				tlsConfigName := "mysql_" + utils.ULID()
				tlsConfigMutex.Lock()
				if err := mysql.RegisterTLSConfig(tlsConfigName, tlsConfig); err != nil {
					tlsConfigMutex.Unlock()
					logger.Errorf("Failed to register TLS config: %v", err)
					cfg.Addr = "invalid-ssl-config:0"
					cfg.TLSConfig = "false"
				} else {
					tlsConfigMutex.Unlock()
					cfg.TLSConfig = tlsConfigName
				}
			}
		}
	}

	if len(c.JDBCURLParams) > 0 {
		if cfg.Params == nil {
			cfg.Params = make(map[string]string)
		}
		maps.Copy(cfg.Params, c.JDBCURLParams)
	}

	return cfg.FormatDSN()
}

// buildTLSConfig builds a custom TLS configuration for certificate-based SSL
func (c *Config) buildTLSConfig() (*tls.Config, error) {
	// This encrypts the connection but doesn't verify server identity
	if c.SSLConfiguration.Mode == utils.SSLModeRequire {
		return &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}, nil
	}

	rootCertPool := x509.NewCertPool()

	if c.SSLConfiguration.ServerCA != "" {
		if ok := rootCertPool.AppendCertsFromPEM([]byte(c.SSLConfiguration.ServerCA)); !ok {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
	}

	tlsConfig := &tls.Config{
		RootCAs:    rootCertPool,
		MinVersion: tls.VersionTLS12,
	}

	if c.SSLConfiguration.Mode == utils.SSLModeVerifyCA {
		// verify-ca: Verify the server's CA certificate but not the hostname
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("no server certificate provided")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("failed to parse server certificate: %w", err)
			}

			// Handle certificate chains with intermediate CAs
			intermediates := x509.NewCertPool()
			for i := 1; i < len(rawCerts); i++ {
				intermediateCert, err := x509.ParseCertificate(rawCerts[i])
				if err != nil {
					logger.Warnf("Failed to parse intermediate certificate at position %d: %v", i, err)
					continue
				}
				intermediates.AddCert(intermediateCert)
			}

			opts := x509.VerifyOptions{
				Roots:         rootCertPool,
				Intermediates: intermediates,
			}
			if _, err := cert.Verify(opts); err != nil {
				return fmt.Errorf("failed to verify server certificate against CA: %w", err)
			}
			return nil
		}
	}

	if c.SSLConfiguration.ClientCert != "" && c.SSLConfiguration.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(c.SSLConfiguration.ClientCert), []byte(c.SSLConfiguration.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate and key: %s", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// Validate checks the configuration for any missing or invalid fields
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https: %s", c.Host)
	}

	// Validate port
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	// Validate required fields
	if c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if c.Password == "" {
		return fmt.Errorf("password is required")
	}

	// Optional database name, default to 'mysql'
	if c.Database == "" {
		c.Database = "mysql"
	}

	// Set default number of threads if not provided
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount // Aligned with PostgreSQL default
	}

	// Set default retry count if not provided
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount // Reasonable default for retries
	}

	// Validate SSL configuration if provided
	if c.SSLConfiguration != nil {
		if err := c.SSLConfiguration.Validate(); err != nil {
			return fmt.Errorf("failed to validate SSL config: %s", err)
		}

		// MySQL-specific SSL validation: for verify-ca and verify-full, ensure ServerCA is present
		if c.SSLConfiguration.Mode == utils.SSLModeVerifyCA || c.SSLConfiguration.Mode == utils.SSLModeVerifyFull {
			if c.SSLConfiguration.ServerCA == "" {
				return fmt.Errorf("'ssl.server_ca' is required for verify-ca and verify-full modes")
			}
		}
	}

	return utils.Validate(c)
}
