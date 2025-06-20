package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/sijms/go-ora/v2"
)

type Config struct {
	Host        string         `json:"hosts"`
	Username    string         `json:"username"`
	Password    string         `json:"password"`
	ServiceName string         `json:"service_name"`
	Port        int            `json:"port"`
	MaxThreads  int            `json:"max_threads"`
	RetryCount  int            `json:"retry_count"`
	DefaultMode types.SyncMode `json:"default_mode"`
}

func (c *Config) connectionString() string {
	return go_ora.BuildUrl(c.Host, c.Port, c.ServiceName, c.Username, c.Password, nil)
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
	if c.ServiceName == "" {
		return fmt.Errorf("service_name is required")
	}

	// Set default number of threads if not provided
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Set default sync mode if not provided
	if c.DefaultMode == "" {
		c.DefaultMode = types.FULLREFRESH
	}

	return utils.Validate(c)
}
