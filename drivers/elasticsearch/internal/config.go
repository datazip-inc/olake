package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
)

// Config holds Elasticsearch connection configuration
type Config struct {
	Host         string      `json:"host"`
	Port         int         `json:"port"`
	Username     string      `json:"username"`
	Password     string      `json:"password"`
	APIKey       string      `json:"api_key"`
	CloudID      string      `json:"cloud_id"`
	UseSSL       bool        `json:"use_ssl"`
	Index        string      `json:"index"`
	UpdateMethod interface{} `json:"update_method"`
	MaxThreads   int         `json:"max_threads"`
	RetryCount   int         `json:"retry_count"`
}

func (c *Config) Validate() error {
	if c.CloudID == "" && c.Host == "" {
		return fmt.Errorf("either host or cloud_id must be provided")
	}

	if c.Host != "" {
		if strings.Contains(c.Host, "https://") || strings.Contains(c.Host, "http://") {
			return fmt.Errorf("host should not contain http or https prefix")
		}

		if c.Port <= 0 || c.Port > 65535 {
			return fmt.Errorf("invalid port number: must be between 1 and 65535")
		}
	}

	// Authentication is optional for testing
	// if c.APIKey == "" && c.Username == "" {
	//	return fmt.Errorf("either api_key or username/password must be provided")
	// }

	if c.Username != "" && c.Password == "" {
		return fmt.Errorf("password is required when username is provided")
	}

	if c.Index == "" {
		return fmt.Errorf("index pattern is required")
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	return nil
}
