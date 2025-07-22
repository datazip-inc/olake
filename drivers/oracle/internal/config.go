package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	go_ora "github.com/sijms/go-ora/v2"
)

type Config struct {
	// Host
	//
	// @jsonSchema(
	//   title="Host",
	//   description="Hostname or IP address of the Oracle database server.",
	//   type="string",
	//   required=true
	// )
	Host string `json:"host"`

	// Username
	//
	// @jsonSchema(
	//   title="Username",
	//   description="Username for authenticating with the Oracle database.",
	//   type="string",
	//   required=true
	// )
	Username string `json:"username"`

	// Password
	//
	// @jsonSchema(
	//   title="Password",
	//   description="Password for the Oracle database user.",
	//   type="string",
	//   format="password",
	//   required=true
	// )
	Password string `json:"password"`

	// Port
	//
	// @jsonSchema(
	//   title="Port",
	//   description="Port number on which the Oracle database is listening.",
	//   type="integer",
	//   required=true
	// )
	Port int `json:"port"`

	// MaxThreads
	//
	// @jsonSchema(
	//   title="Max Threads",
	//   description="Max parallel threads for chunk snapshotting",
	//   type="integer",
	//   default=3
	// )
	MaxThreads int `json:"max_threads"`

	// RetryCount
	//
	// @jsonSchema(
	//   title="Retry Count",
	//   description="Number of sync retry attempts using exponential backoff",
	//   type="integer",
	//   default=3
	// )
	RetryCount int `json:"backoff_retry_count"`

	// SSLConfiguration
	//
	// @jsonSchema(
	//   title="SSL Mode",
	//   description="Database connection SSL configuration (e.g., SSL mode)"
	// )
	SSLConfiguration *utils.SSLConfig `json:"ssl"`

	// JDBCURLParams
	//
	// @jsonSchema(
	//   title="JDBC URL Parameters",
	//   description="Additional JDBC URL params for connection tuning (optional)",
	//   additionalProperties="string"
	// )
	JDBCURLParams map[string]string `json:"jdbc_url_params"`

	// ConnectionType
	//
	// @jsonSchema(
	//   title="Connection Type",
	//   description="Connection type for Oracle database (e.g., SID or Service Name)",
	//   oneOf=["SID","ServiceName"]
	// )
	ConnectionType interface{} `json:"connection_type"`
}

// SID
//
// @jsonSchema(
//
//	title="SID"
//
// )
type SID struct {
	// @jsonSchema(
	//   title="SID",
	//   description="The Oracle database SID to connect to",
	//   type="string",
	//   required=true
	// )
	SID string `json:"sid"`
}

// ServiceName
//
// @jsonSchema(
//
//	title="Service Name"
//
// )
type ServiceName struct {
	// @jsonSchema(
	//   title="Service Name",
	//   description="The Oracle database service name to connect to",
	//   type="string",
	//   required=true
	// )
	ServiceName string `json:"service_name"`
}

func (c *Config) connectionString() (string, error) {
	urlOptions := make(map[string]string)
	// Add JDBC-style URL params
	for k, v := range c.JDBCURLParams {
		urlOptions[k] = v
	}

	serviceName := ""
	found, _ := utils.IsOfType(c.ConnectionType, "sid")
	if found {
		unmarshalledSID := &SID{}
		if err := utils.Unmarshal(c.ConnectionType, unmarshalledSID); err != nil {
			return "", fmt.Errorf("failed to unmarshal sid: %s", err)
		}
		urlOptions["SID"] = unmarshalledSID.SID
	} else {
		unmarshalledServiceName := &ServiceName{}
		if err := utils.Unmarshal(c.ConnectionType, unmarshalledServiceName); err != nil {
			return "", fmt.Errorf("failed to unmarshal service name: %s", err)
		}
		serviceName = unmarshalledServiceName.ServiceName
	}

	// Add SSL params if provided
	if c.SSLConfiguration != nil {
		sslmode := string(c.SSLConfiguration.Mode)
		if sslmode != "disable" {
			urlOptions["ssl"] = "true"
			urlOptions["ssl verify"] = "false"
		}
		// TODO: Add support for more SSL params
	}

	// Quote the username to handle case sensitivity
	quotedUsername := fmt.Sprintf("%q", c.Username)

	return go_ora.BuildUrl(c.Host, c.Port, serviceName, quotedUsername, c.Password, urlOptions), nil
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

	foundServiceName, _ := utils.IsOfType(c.ConnectionType, "service_name")
	foundSID, _ := utils.IsOfType(c.ConnectionType, "sid")
	if !foundServiceName && !foundSID {
		return fmt.Errorf("service name or sid is required")
	} else if foundServiceName && foundSID {
		return fmt.Errorf("only one of service name or sid can be provided")
	}

	// Set default number of threads if not provided
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: "disable",
		}
	}
	err := c.SSLConfiguration.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate ssl config: %s", err)
	}
	return utils.Validate(c)
}
