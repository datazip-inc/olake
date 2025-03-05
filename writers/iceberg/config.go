package iceberg

import (
	"fmt"
	"os"
	"strings"

	"github.com/datazip-inc/olake/logger"
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
		// Set JarPath based on file existence in two possible locations
		execDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current directory: %v", err)
		}

		logger.Infof("Current working directory: %s", execDir)

		// Remove /drivers/* from execDir if present
		if idx := strings.LastIndex(execDir, "/drivers/"); idx != -1 {
			execDir = execDir[:idx]
		}

		logger.Infof("Current working directory: %s", execDir)

		// First, check if the JAR exists in the base directory
		baseJarPath := fmt.Sprintf("%s/debezium-server-iceberg-sink.jar", execDir)
		if _, err := os.Stat(baseJarPath); err == nil {
			// JAR file exists in base directory
			logger.Infof("Iceberg JAR file found in base directory: %s", baseJarPath)
			c.JarPath = baseJarPath
		} else {
			// Otherwise, look in the target directory
			targetJarPath := fmt.Sprintf("%s/writers/iceberg/debezium-server-iceberg-sink/target/debezium-server-iceberg-sink-0.0.1-SNAPSHOT.jar", execDir)
			if _, err := os.Stat(targetJarPath); err == nil {
				logger.Infof("Iceberg JAR file found in target directory: %s", targetJarPath)
				c.JarPath = targetJarPath
			} else {
				// Check the previous location as last resort
				fallbackPath := fmt.Sprintf("%s/debezium-server-iceberg-sink-0.0.1-SNAPSHOT.jar", execDir)
				if _, err := os.Stat(fallbackPath); err == nil {
					logger.Infof("Iceberg JAR file found in fallback location: %s", fallbackPath)
					c.JarPath = fallbackPath
				} else {
					// Throw error if JAR is not found in any location
					return fmt.Errorf("Iceberg JAR file not found in any of the expected locations: %s, %s, or %s",
						baseJarPath, targetJarPath, fallbackPath)
				}
			}
		}
	}
	if c.ServerHost == "" {
		c.ServerHost = "localhost"
	}
	return utils.Validate(c)
}
