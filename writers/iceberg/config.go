package iceberg

import (
	"fmt"
	"os"
	"strings"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
)

// CatalogType represents supported Iceberg catalog implementations
type CatalogType string

const (
	// GlueCatalog is the AWS Glue catalog implementation
	GlueCatalog CatalogType = "glue"
	// JDBCCatalog is the JDBC catalog implementation
	JDBCCatalog CatalogType = "jdbc"
	// HiveCatalog is the Hive catalog implementation
	HiveCatalog CatalogType = "hive"
	// RestCatalog is the REST catalog implementation
	RestCatalog CatalogType = "rest"
)

type Config struct {
	// S3-compatible Storage Configuration
	Region       string `json:"aws_region,omitempty"`
	AccessKey    string `json:"aws_access_key,omitempty"`
	SecretKey    string `json:"aws_secret_key,omitempty"`
	SessionToken string `json:"aws_session_token,omitempty"`
	ProfileName  string `json:"aws_profile,omitempty"`
	// S3 endpoint for custom S3-compatible services (like MinIO)
	S3Endpoint  string `json:"s3_endpoint,omitempty"`
	S3UseSSL    bool   `json:"s3_use_ssl,omitempty"`    // Use HTTPS if true
	S3PathStyle bool   `json:"s3_path_style,omitempty"` // Use path-style instead of virtual-hosted-style https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html

	// Catalog Configuration
	CatalogType CatalogType `json:"catalog_type,omitempty"`
	CatalogName string      `json:"catalog_name,omitempty"`
	// JDBC specific configuration
	JDBCUrl      string `json:"jdbc_url,omitempty"`
	JDBCUsername string `json:"jdbc_username,omitempty"`
	JDBCPassword string `json:"jdbc_password,omitempty"`

	// Hive specific configuration
	HiveURI         string `json:"hive_uri,omitempty"`
	HiveClients     int    `json:"hive_clients,omitempty"`
	HiveSaslEnabled bool   `json:"hive_sasl_enabled,omitempty"`

	// Rest Catalog Configuration
	RestCatalogURL string `json:"rest_catalog_url,omitempty"`

	// Iceberg Configuration
	IcebergDatabase string `json:"iceberg_db,omitempty"`
	IcebergS3Path   string `json:"iceberg_s3_path"`                // e.g. s3://bucket/path
	JarPath         string `json:"sink_jar_path,omitempty"`        // Path to the Iceberg sink JAR
	ServerHost      string `json:"sink_rpc_server_host,omitempty"` // gRPC server host

}

func (c *Config) Validate() error {
	if c.IcebergDatabase == "" {
		c.IcebergDatabase = "olake_iceberg"
	}
	if c.IcebergS3Path == "" {
		return fmt.Errorf("s3_path is required")
	}

	// Set defaults for catalog type
	if c.CatalogType == "" {
		c.CatalogType = GlueCatalog
	}

	if c.CatalogName == "" {
		c.CatalogName = "olake_iceberg"
	}

	// Default to path-style access for S3-compatible services
	if c.S3Endpoint != "" {
		c.S3PathStyle = true
	}

	// Validate S3 configuration
	// Region can be picked up from environment or credentials file for AWS S3
	if c.S3Endpoint == "" && c.Region == "" {
		logger.Warn("aws_region not explicitly provided, will attempt to use region from environment variables or AWS config/credentials file")
	}

	// Log information about credentials for all S3 configurations
	if c.AccessKey == "" && c.SecretKey == "" && c.ProfileName == "" {
		if c.S3Endpoint == "" {
			// AWS S3 scenario
			logger.Info("AWS credentials not explicitly provided, will use default credential chain (environment variables, AWS config/credentials file, or instance metadata service)")
		} else {
			// Custom S3 endpoint scenario
			logger.Info("S3 credentials not explicitly provided for custom endpoint. Ensure the service supports anonymous access or credentials are available through other means")
		}
	}

	// Validate based on catalog type
	switch c.CatalogType {
	case JDBCCatalog:
		if c.JDBCUrl == "" {
			return fmt.Errorf("jdbc_url is required when using JDBC catalog")
		}
	case RestCatalog:
		if c.RestCatalogURL == "" {
			return fmt.Errorf("rest_catalog_url is required when using REST catalog")
		}
	case HiveCatalog:
		if c.HiveURI == "" {
			return fmt.Errorf("hive_uri is required when using Hive catalog")
		}
	case GlueCatalog:
		// No additional validation required for Glue catalog
	default:
		return fmt.Errorf("unsupported catalog_type: %s", c.CatalogType)
	}

	if c.JarPath == "" {
		// Set JarPath based on file existence in two possible locations
		execDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current directory for searching jar file: %v", err)
		}

		// Remove /drivers/* from execDir if present
		if idx := strings.LastIndex(execDir, "/drivers/"); idx != -1 {
			execDir = execDir[:idx]
		}

		// First, check if the JAR exists in the base directory
		baseJarPath := fmt.Sprintf("%s/debezium-server-iceberg-sink.jar", execDir)
		if err := utils.CheckIfFilesExists(baseJarPath); err == nil {
			// JAR file exists in base directory
			logger.Infof("Iceberg JAR file found in base directory: %s", baseJarPath)
			c.JarPath = baseJarPath
		} else {
			// Otherwise, look in the target directory
			targetJarPath := fmt.Sprintf("%s/writers/iceberg/debezium-server-iceberg-sink/target/debezium-server-iceberg-sink-0.0.1-SNAPSHOT.jar", execDir)
			if err := utils.CheckIfFilesExists(targetJarPath); err == nil {
				logger.Infof("Iceberg JAR file found in target directory: %s", targetJarPath)
				c.JarPath = targetJarPath
			} else {
				return fmt.Errorf("Iceberg JAR file not found in any of the expected locations: %s, %s. Go to writers/iceberg/debezium-server-iceberg-sink/target/ directory and run mvn clean package -DskipTests",
					baseJarPath, targetJarPath)
			}
		}
	}
	if c.ServerHost == "" {
		c.ServerHost = "localhost"
	}
	return utils.Validate(c)
}
