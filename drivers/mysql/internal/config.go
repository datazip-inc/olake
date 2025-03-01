package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to a MySQL database
type Config struct {
	Hosts         []string       `json:"hosts"`
	Username      string         `json:"username"`
	Password      string         `json:"password"`
	Database      string         `json:"database"`
	Port          int            `json:"port"`
	TLSSkipVerify bool           `json:"tls_skip_verify"` // Add this field
	DefaultMode   types.SyncMode `json:"default_mode"`
}

// URI generates the connection URI for the MySQL database
func (c *Config) URI() string {
	// Set default port if not specified
	if c.Port == 0 {
		c.Port = 3306
	}

	// Construct connection parameters
	params := []string{
		"parseTime=true", // Enable parsing of TIME/DATE/DATETIME columns
		fmt.Sprintf("maxAllowedPacket=%d", 16<<20), // 16MB max packet size
	}

	// Handle TLS configuration
	if c.TLSSkipVerify {
		params = append(params, "tls=skip-verify") // Skip certificate verification
	} else {
		params = append(params, "tls=true") // Use standard TLS verification
	}

	// Construct host string
	hostStr := strings.Join(c.Hosts, ",")
	if len(c.Hosts) == 0 {
		hostStr = "localhost"
	}

	// Construct full connection string
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?%s",
		c.Username,
		c.Password,
		hostStr,
		c.Port,
		c.Database,
		strings.Join(params, "&"),
	)
}

// Validate checks the configuration for any missing or invalid fields
func (c *Config) Validate() error {
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

	return utils.Validate(c)
}
