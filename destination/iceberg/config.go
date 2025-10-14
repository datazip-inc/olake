package iceberg

import (
	"fmt"
	"os"
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
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

// TODO: add validation for each catalog properly
type Config struct {
	// S3-compatible Storage Configuration
	Region             string `json:"aws_region" validate:"required_if=CatalogType glue"`
	AccessKey          string `json:"aws_access_key" validate:"required_with=SecretKey"`
	SecretKey          string `json:"aws_secret_key" validate:"required_with=AccessKey"`
	SessionToken       string `json:"aws_session_token" validate:"omitempty"`
	ProfileName        string `json:"aws_profile" validate:"omitempty,excluded_with=AccessKey"`
	NoIdentifierFields bool   `json:"no_identifier_fields"` // Needed to set true for Databricks Unity Catalog as it doesn't support identifier fields

	// S3 endpoint for custom S3-compatible services (like MinIO)
	S3Endpoint  string `json:"s3_endpoint" validate:"omitempty,url"`
	S3UseSSL    bool   `json:"s3_use_ssl"`    // Use HTTPS if true
	S3PathStyle bool   `json:"s3_path_style"` // Use path-style instead of virtual-hosted-style https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html

	// Catalog Configuration
	CatalogType CatalogType `json:"catalog_type" validate:"required,oneof=glue jdbc hive rest"`
	CatalogName string      `json:"catalog_name" validate:"required"`

	// JDBC specific configuration
	JDBCUrl      string `json:"jdbc_url" validate:"required_if=CatalogType jdbc,url"`
	JDBCUsername string `json:"jdbc_username" validate:"required_if=CatalogType jdbc"`
	JDBCPassword string `json:"jdbc_password" validate:"required_if=CatalogType jdbc"`

	// Hive specific configuration
	HiveURI         string `json:"hive_uri" validate:"required_if=CatalogType hive,url"`
	HiveClients     int    `json:"hive_clients" validate:"omitempty,min=1"`
	HiveSaslEnabled bool   `json:"hive_sasl_enabled"`

	// Iceberg Configuration
	IcebergDatabase string `json:"iceberg_db" validate:"omitempty"`
	IcebergS3Path   string `json:"iceberg_s3_path" validate:"required,startswith=s3://"` // e.g. s3://bucket/path
	JarPath         string `json:"sink_jar_path" validate:"omitempty,file"`             // Path to the Iceberg sink JAR
	ServerHost      string `json:"sink_rpc_server_host" validate:"omitempty,hostname"`  // gRPC server host

	// Rest Catalog Configuration
	RestCatalogURL    string `json:"rest_catalog_url" validate:"required_if=CatalogType rest,url"`
	RestSigningName   string `json:"rest_signing_name,omitempty"`
	RestSigningRegion string `json:"rest_signing_region" validate:"required_if=RestSigningV4 true"`
	RestSigningV4     bool   `json:"rest_signing_v_4"`
	RestToken         string `json:"token" validate:"required_if=RestAuthType token"`
	RestOAuthURI      string `json:"oauth2_uri" validate:"required_if=RestAuthType oauth2,omitempty,url"`
	RestAuthType      string `json:"rest_auth_type" validate:"omitempty,oneof=none token oauth2"`
	RestScope         string `json:"scope" validate:"required_if=RestAuthType oauth2"`
	RestCredential    string `json:"credential" validate:"required_if=RestAuthType oauth2"`
}

func (c *Config) Validate() error {
	// Set defaults for catalog type
	if c.CatalogType == "" {
		c.CatalogType = GlueCatalog
	}

	if c.CatalogName == "" {
		c.CatalogName = "olake_iceberg"
	}

	if c.ServerHost == "" {
		c.ServerHost = "localhost"
	}
	
	// Default to path-style access for S3-compatible services
	if c.S3Endpoint != "" {
		c.S3PathStyle = true
	}

	// Set Hive defaults
	if c.CatalogType == HiveCatalog && c.HiveClients <= 0 {
		c.HiveClients = 5
	}

	// Validate S3 configuration
	// Region can be picked up from environment or credentials file for AWS S3
	if c.S3Endpoint == "" && c.Region == "" {
		logger.Warn("aws_region not explicitly provided, will attempt to use region from environment variables or AWS config/credentials file")
	}

	if c.AccessKey == "" && c.SecretKey == "" && c.ProfileName == "" {
		if c.S3Endpoint == "" {
			logger.Info("AWS credentials not explicitly provided, will use default credential chain")
		} else {
			logger.Info("S3 credentials not explicitly provided for custom endpoint")
		}
	}

	// Basic validation
	if c.IcebergS3Path == "" {
		return fmt.Errorf("s3_path is required")
	}

	// Validate based on catalog type
	switch c.CatalogType {
	case JDBCCatalog:
		if !strings.HasPrefix(c.JDBCUrl, "jdbc:") {
			return fmt.Errorf("invalid JDBC URL format: must start with 'jdbc:'")
		}
	case RestCatalog:
		if c.RestSigningV4 && (c.RestSigningRegion == "" || c.RestSigningName == "") {
			return fmt.Errorf("rest_signing_name and rest_signing_region are required when rest_signing_v4 is enabled")
		}
		
		switch c.RestAuthType {
		case "token":
			if c.RestToken == "" {
				return fmt.Errorf("token is required when using token authentication")
			}
		case "oauth2":
			if c.RestOAuthURI == "" || c.RestScope == "" || c.RestCredential == "" {
				return fmt.Errorf("oauth2_uri, scope, and credential are required for OAuth2 authentication")
			}
		case "", "none":
		default:
			return fmt.Errorf("unsupported REST authentication type: %s", c.RestAuthType)
		}
	case HiveCatalog:
		if !strings.HasPrefix(c.HiveURI, "thrift://") {
			return fmt.Errorf("invalid Hive URI format: must start with 'thrift://'")
		}
		if c.HiveClients > 20 {
			logger.Warn("High number of Hive clients configured (%d). This may impact performance", c.HiveClients)
		}
	case GlueCatalog:
		if c.AccessKey != "" || c.SecretKey != "" {
			if c.AccessKey == "" || c.SecretKey == "" {
				return fmt.Errorf("both aws_access_key and aws_secret_key must be provided when using explicit AWS credentials")
			}
		}
	default:
		return fmt.Errorf("unsupported catalog_type: %s", c.CatalogType)
	}

	if c.JarPath == "" {
		// Set JarPath based on file existence in two possible locations
		execDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current directory for searching jar file: %s", err)
		}

		// Remove /drivers/* from execDir if present
		if idx := strings.LastIndex(execDir, "/drivers/"); idx != -1 {
			execDir = execDir[:idx]
		}

		// First, check if the JAR exists in the base directory
		baseJarPath := fmt.Sprintf("%s/olake-iceberg-java-writer.jar", execDir)
		if err := utils.CheckIfFilesExists(baseJarPath); err == nil {
			// JAR file exists in base directory
			logger.Infof("Iceberg JAR file found in base directory: %s", baseJarPath)
			c.JarPath = baseJarPath
		} else {
			// Otherwise, look in the target directory
			targetJarPath := fmt.Sprintf("%s/destination/iceberg/olake-iceberg-java-writer/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar", execDir)
			if err := utils.CheckIfFilesExists(targetJarPath); err == nil {
				logger.Infof("Iceberg JAR file found in target directory: %s", targetJarPath)
				c.JarPath = targetJarPath
			} else {
				return fmt.Errorf("Iceberg JAR file not found in any of the expected locations: %s, %s. Go to destination/iceberg/olake-iceberg-java-writer/target/ directory and run mvn clean package -DskipTests",
					baseJarPath, targetJarPath)
			}
		}
	}
	return utils.Validate(c)
}
