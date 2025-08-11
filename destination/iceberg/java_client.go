package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type serverInstance struct {
	port     int
	cmd      *exec.Cmd
	client   proto.RecordIngestServiceClient
	conn     *grpc.ClientConn
	upsert   bool
	serverID string
}

// getServerConfigJSON generates the JSON configuration for the Iceberg server
func getServerConfigJSON(config *Config, partitionInfo []PartitionInfo, port int, upsert bool) ([]byte, error) {
	// Create the server configuration map
	serverConfig := map[string]interface{}{
		"port":                     fmt.Sprintf("%d", port),
		"warehouse":                config.IcebergS3Path,
		"table-namespace":          config.IcebergDatabase,
		"catalog-name":             "olake_iceberg",
		"table-prefix":             "",
		"create-identifier-fields": !config.NoIdentifierFields,
		"upsert":                   strconv.FormatBool(upsert),
		"upsert-keep-deletes":      "true",
		"write.format.default":     "parquet",
	}

	// Add partition fields as an array to preserve order
	if len(partitionInfo) > 0 {
		partitionFields := make([]map[string]string, 0, len(partitionInfo))
		for _, info := range partitionInfo {
			partitionFields = append(partitionFields, map[string]string{
				"field":     info.field,
				"transform": info.transform,
			})
		}
		serverConfig["partition-fields"] = partitionFields
	}

	addMapKeyIfNotEmpty := func(key, value string) {
		if value != "" {
			serverConfig[key] = value
		}
	}
	// Configure catalog implementation based on the selected type
	switch config.CatalogType {
	case GlueCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
	case JDBCCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.jdbc.JdbcCatalog"
		serverConfig["uri"] = config.JDBCUrl
		addMapKeyIfNotEmpty("jdbc.user", config.JDBCUsername)
		addMapKeyIfNotEmpty("jdbc.password", config.JDBCPassword)
	case HiveCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.hive.HiveCatalog"
		serverConfig["uri"] = config.HiveURI
		serverConfig["clients"] = strconv.Itoa(config.HiveClients)
		serverConfig["hive.metastore.sasl.enabled"] = strconv.FormatBool(config.HiveSaslEnabled)
		serverConfig["engine.hive.enabled"] = "true"
	case RestCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.rest.RESTCatalog"
		serverConfig["uri"] = config.RestCatalogURL
		serverConfig["rest.sigv4-enabled"] = strconv.FormatBool(config.RestSigningV4)
		addMapKeyIfNotEmpty("rest.signing-name", config.RestSigningName)
		addMapKeyIfNotEmpty("rest.signing-region", config.RestSigningRegion)
		addMapKeyIfNotEmpty("token", config.RestToken)
		addMapKeyIfNotEmpty("oauth2-server-uri", config.RestOAuthURI)
		addMapKeyIfNotEmpty("rest.auth.type", config.RestAuthType)
		addMapKeyIfNotEmpty("credential", config.RestCredential)
		addMapKeyIfNotEmpty("scope", config.RestScope)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", config.CatalogType)
	}

	// Configure S3 file IO
	serverConfig["io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"

	// Only set access keys if explicitly provided, otherwise they'll be picked up from
	// environment variables or AWS credential files
	serverConfig["s3.path-style-access"] = utils.Ternary(config.S3PathStyle, "true", "false").(string)
	addMapKeyIfNotEmpty("s3.access-key-id", config.AccessKey)
	addMapKeyIfNotEmpty("s3.secret-access-key", config.SecretKey)
	addMapKeyIfNotEmpty("aws.profile", config.ProfileName)
	addMapKeyIfNotEmpty("aws.session-token", config.SessionToken)

	// Configure region for AWS S3
	if config.Region != "" {
		serverConfig["s3.region"] = config.Region
	} else if config.S3Endpoint == "" && config.CatalogType == GlueCatalog {
		// If no region is explicitly provided for Glue catalog, add a note that it will be picked from environment
		logger.Warn("No region explicitly provided for Glue catalog, the Java process will attempt to use region from AWS environment")
	}

	// Configure custom endpoint for S3-compatible services (like MinIO)
	if config.S3Endpoint != "" {
		serverConfig["s3.endpoint"] = config.S3Endpoint
		serverConfig["io-impl"] = "org.apache.iceberg.io.ResolvingFileIO"
		// Set SSL/TLS configuration
		serverConfig["s3.ssl-enabled"] = utils.Ternary(config.S3UseSSL, "true", "false").(string)
	}

	// Marshal the config to JSON
	return json.Marshal(serverConfig)
}

// setup java client
func newIcebergClient(config *Config, partitionInfo []PartitionInfo, check, upsert bool) (*serverInstance, error) {
	// validate configuration
	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("failed to validate config: %s", err)
	}

	// get thread id first
	threadID := destination.GetGoroutineID()
	// get available port
	port, err := destination.FindAvailablePort(config.ServerHost)
	if err != nil {
		return nil, fmt.Errorf("failed to find available ports: %s", err)
	}

	// Get the server configuration JSON
	configJSON, err := getServerConfigJSON(config, partitionInfo, port, upsert)
	if err != nil {
		return nil, fmt.Errorf("failed to create server config: %s", err)
	}

	// setup command
	var serverCmd *exec.Cmd
	// If debug mode is enabled and stream is available (stream is nil for check operations), start the server with debug options
	if os.Getenv("OLAKE_DEBUG_MODE") != "" && check {
		serverCmd = exec.Command("java", "-XX:+UseG1GC", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005", "-jar", config.JarPath, string(configJSON))
	} else {
		serverCmd = exec.Command("java", "-XX:+UseG1GC", "-jar", config.JarPath, string(configJSON))
	}

	// Get current environment
	serverCmd.Env = utils.Ternary(serverCmd.Env == nil, []string{}, serverCmd.Env).([]string)

	addEnvIfSet := func(key, value string) {
		if value != "" {
			serverCmd.Env = append(serverCmd.Env, fmt.Sprintf("%s=%s", key, value))
		}
	}
	addEnvIfSet("AWS_ACCESS_KEY_ID", config.AccessKey)
	addEnvIfSet("AWS_SECRET_ACCESS_KEY", config.SecretKey)
	addEnvIfSet("AWS_REGION", config.Region)
	addEnvIfSet("AWS_SESSION_TOKEN", config.SessionToken)
	addEnvIfSet("AWS_PROFILE", config.ProfileName)

	// Set up and start the process with logging
	if err := logger.SetupAndStartProcess(fmt.Sprintf("Java-Iceberg:%d", port), serverCmd); err != nil {
		return nil, fmt.Errorf("failed to setup logger: %s", err)
	}

	// Connect to gRPC server
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", config.ServerHost, strconv.Itoa(port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	if err != nil {
		// If connection fails, clean up the process
		if serverCmd != nil && serverCmd.Process != nil {
			if killErr := serverCmd.Process.Kill(); killErr != nil {
				logger.Errorf("thread id %s: Failed to kill process: %s", threadID, killErr)
			}
		}
		return nil, fmt.Errorf("failed to create new grpc client: %s", err)
	}

	logger.Infof("thread id %s: Connected to new iceberg writer on port %d", threadID, port)
	return &serverInstance{
		port:     port,
		cmd:      serverCmd,
		client:   proto.NewRecordIngestServiceClient(conn),
		conn:     conn,
		serverID: threadID,
	}, nil
}

func (s *serverInstance) sendClientRequest(ctx context.Context, reqPayload *proto.IcebergPayload) (string, error) {
	resp, err := s.client.SendRecords(ctx, reqPayload)
	if err != nil {
		return "", fmt.Errorf("failed to send grpc request: %s", err)
	}
	return resp.GetResult(), nil
}

// closeIcebergClient closes the connection to the Iceberg server
func (s *serverInstance) closeIcebergClient(server *serverInstance) error {
	// If this was the last reference, shut down the server
	logger.Infof("thread id %s: shutting down Iceberg server on port %d", server.serverID, server.port)
	server.conn.Close()
	if server.cmd != nil && server.cmd.Process != nil {
		err := server.cmd.Process.Kill()
		if err != nil {
			logger.Errorf("thread id %s: Failed to kill Iceberg server: %s", server.serverID, err)
		}
	}

	return nil
}
