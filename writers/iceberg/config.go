package iceberg

import (
	"fmt"
	"os"
	"strings"

	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	// AWS Configuration
	Region       string `json:"aws_region"`
	AccessKey    string `json:"aws_access_key,omitempty"`
	SecretKey    string `json:"aws_secret_key,omitempty"`
	SessionToken string `json:"aws_session_token,omitempty"`
	ProfileName  string `json:"aws_profile,omitempty"`

	// Iceberg Configuration
	Database   string `json:"database"`
	S3Path     string `json:"s3_path"`     // e.g. s3://bucket/path
	JarPath    string `json:"jar_path"`    // Path to the Iceberg sink JAR
	ServerHost string `json:"server_host"` // gRPC server host

	Normalization bool `json:"normalization,omitempty"`
}

func (c *Config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("aws_region is required")
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}
	if c.S3Path == "" {
		return fmt.Errorf("s3_path is required")
	}
	if c.JarPath == "" {
		// Set JarPath to the current directory + the path to the jar file
		execDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current directory: %v", err)
		}
		// Remove /drivers/* from execDir if present
		if idx := strings.LastIndex(execDir, "/drivers/"); idx != -1 {
			execDir = execDir[:idx]
		}
		c.JarPath = fmt.Sprintf("%s/debezium-server-iceberg-sink-0.0.1-SNAPSHOT.jar", execDir)
	}
	if c.ServerHost == "" {
		c.ServerHost = "localhost"
	}
	return utils.Validate(c)
}
