package driver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)
// Config represents the configuration for connecting to a MySQL database
type Config struct {
	Host          string      `json:"hosts"`
	Username      string      `json:"username"`
	Password      string      `json:"password"`
	Database      string      `json:"database"`
	Port          int         `json:"port"`
	TLSSkipVerify bool        `json:"tls_skip_verify"` // Add this field
	UpdateMethod  interface{} `json:"update_method"`
	MaxThreads    int         `json:"max_threads"`
	RetryCount    int         `json:"backoff_retry_count"`
	JDBCURLParams map[string]string `json:"jdbc_url_params,omitempty"`
	SSLConfig     *utils.SSLConfig  `json:"ssl_config,omitempty"`
}
type CDC struct {
	InitialWaitTime int `json:"intial_wait_time"`
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
	// Handle SSL configuration (only if present)
	urlParams := url.Values{}
	if c.SSLConfig != nil && c.SSLConfig.Mode != utils.Unknown && c.SSLConfig.Mode != utils.SSLModeDisable {
		// Compose TLS config
		rootCertPool := x509.NewCertPool()
		if c.SSLConfig.ServerCA != "" {
			pem, err := os.ReadFile(c.SSLConfig.ServerCA)
			if err == nil {
				rootCertPool.AppendCertsFromPEM(pem)
			}
		}

		clientCert := make([]tls.Certificate, 0, 1)
		if c.SSLConfig.ClientCert != "" && c.SSLConfig.ClientKey != "" {
			cert, err := tls.LoadX509KeyPair(c.SSLConfig.ClientCert, c.SSLConfig.ClientKey)
			if err == nil {
				clientCert = append(clientCert, cert)
			}
		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify: c.TLSSkipVerify,
			RootCAs:            rootCertPool,
			Certificates:       clientCert,
		}
		mysql.RegisterTLSConfig("custom", tlsConfig)
		urlParams.Set("tls", "custom")
	}

	// Add custom JDBC URL parameters
	for k, v := range c.JDBCURLParams {
		urlParams.Set(k, v)
	}

	// Build DSN with query params if any
	dsn := cfg.FormatDSN()
	if len(urlParams) > 0 {
		if strings.Contains(dsn, "?") {
			dsn = fmt.Sprintf("%s&%s", dsn, urlParams.Encode())
		} else {
			dsn = fmt.Sprintf("%s?%s", dsn, urlParams.Encode())
		}
	}
	return dsn
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
	if c.SSLConfig != nil {
		if err := c.SSLConfig.Validate(); err != nil {
			return fmt.Errorf("invalid SSL config: %w", err)
		}
	}
	return utils.Validate(c)
}
