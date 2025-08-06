package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// getGoroutineID returns a unique ID for the current goroutine
// This is a simple implementation that uses the string address of a local variable
// which will be unique per goroutine
func getGoroutineID() string {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	id := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	return id
}

// getServerConfigJSON generates the JSON configuration for the Iceberg server
func (i *Iceberg) getServerConfigJSON(port int, upsert bool) ([]byte, error) {
	// Create the server configuration map
	serverConfig := map[string]interface{}{
		"port":                     fmt.Sprintf("%d", port),
		"warehouse":                i.config.IcebergS3Path,
		"table-namespace":          i.config.IcebergDatabase,
		"catalog-name":             "olake_iceberg",
		"table-prefix":             "",
		"create-identifier-fields": !i.config.NoIdentifierFields,
		"upsert":                   strconv.FormatBool(upsert),
		"upsert-keep-deletes":      "true",
		"write.format.default":     "parquet",
	}

	// Add partition fields as an array to preserve order
	if len(i.partitionInfo) > 0 {
		partitionFields := make([]map[string]string, 0, len(i.partitionInfo))
		for _, info := range i.partitionInfo {
			partitionFields = append(partitionFields, map[string]string{
				"field":     info.Field,
				"transform": info.Transform,
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
	switch i.config.CatalogType {
	case GlueCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
	case JDBCCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.jdbc.JdbcCatalog"
		serverConfig["uri"] = i.config.JDBCUrl
		addMapKeyIfNotEmpty("jdbc.user", i.config.JDBCUsername)
		addMapKeyIfNotEmpty("jdbc.password", i.config.JDBCPassword)
	case HiveCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.hive.HiveCatalog"
		serverConfig["uri"] = i.config.HiveURI
		serverConfig["clients"] = strconv.Itoa(i.config.HiveClients)
		serverConfig["hive.metastore.sasl.enabled"] = strconv.FormatBool(i.config.HiveSaslEnabled)
		serverConfig["engine.hive.enabled"] = "true"
	case RestCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.rest.RESTCatalog"
		serverConfig["uri"] = i.config.RestCatalogURL
		serverConfig["rest.sigv4-enabled"] = strconv.FormatBool(i.config.RestSigningV4)
		addMapKeyIfNotEmpty("rest.signing-name", i.config.RestSigningName)
		addMapKeyIfNotEmpty("rest.signing-region", i.config.RestSigningRegion)
		addMapKeyIfNotEmpty("token", i.config.RestToken)
		addMapKeyIfNotEmpty("oauth2-server-uri", i.config.RestOAuthURI)
		addMapKeyIfNotEmpty("rest.auth.type", i.config.RestAuthType)
		addMapKeyIfNotEmpty("credential", i.config.RestCredential)
		addMapKeyIfNotEmpty("scope", i.config.RestScope)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", i.config.CatalogType)
	}

	// Configure S3 file IO
	serverConfig["io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"

	// Only set access keys if explicitly provided, otherwise they'll be picked up from
	// environment variables or AWS credential files
	serverConfig["s3.path-style-access"] = utils.Ternary(i.config.S3PathStyle, "true", "false").(string)
	addMapKeyIfNotEmpty("s3.access-key-id", i.config.AccessKey)
	addMapKeyIfNotEmpty("s3.secret-access-key", i.config.SecretKey)
	addMapKeyIfNotEmpty("aws.profile", i.config.ProfileName)
	addMapKeyIfNotEmpty("aws.session-token", i.config.SessionToken)

	// Configure region for AWS S3
	if i.config.Region != "" {
		serverConfig["s3.region"] = i.config.Region
	} else if i.config.S3Endpoint == "" && i.config.CatalogType == GlueCatalog {
		// If no region is explicitly provided for Glue catalog, add a note that it will be picked from environment
		logger.Warn("No region explicitly provided for Glue catalog, the Java process will attempt to use region from AWS environment")
	}

	// Configure custom endpoint for S3-compatible services (like MinIO)
	if i.config.S3Endpoint != "" {
		serverConfig["s3.endpoint"] = i.config.S3Endpoint
		serverConfig["io-impl"] = "org.apache.iceberg.io.ResolvingFileIO"
		// Set SSL/TLS configuration
		serverConfig["s3.ssl-enabled"] = utils.Ternary(i.config.S3UseSSL, "true", "false").(string)
	}

	// Marshal the config to JSON
	return json.Marshal(serverConfig)
}

// Initialize the local buffer in the Iceberg instance during setup
func (i *Iceberg) SetupIcebergClient(upsert bool) error {
	// Create JSON config for the Java server
	err := i.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// get available port
	port, err := destination.FindAvailablePort(i.config.ServerHost)
	if err != nil {
		return err
	}

	// Get the server configuration JSON
	configJSON, err := i.getServerConfigJSON(port, upsert)
	if err != nil {
		return fmt.Errorf("failed to create server config: %s", err)
	}

	var serverCmd *exec.Cmd
	// If debug mode is enabled and stream is available (stream is nil for check operations), start the server with debug options
	if os.Getenv("OLAKE_DEBUG_MODE") != "" && i.stream != nil {
		serverCmd = exec.Command("java", "-XX:+UseG1GC", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005", "-jar", i.config.JarPath, string(configJSON))
	} else {
		serverCmd = exec.Command("java", "-XX:+UseG1GC", "-jar", i.config.JarPath, string(configJSON))
	}

	// Get current environment
	env := utils.Ternary(serverCmd.Env == nil, []string{}, serverCmd.Env).([]string)
	addEnvIfSet := func(key, value string) {
		if value != "" {
			env = append(env, fmt.Sprintf("%s=%s", key, value))
		}
	}
	addEnvIfSet("AWS_ACCESS_KEY_ID", i.config.AccessKey)
	addEnvIfSet("AWS_SECRET_ACCESS_KEY", i.config.SecretKey)
	addEnvIfSet("AWS_REGION", i.config.Region)
	addEnvIfSet("AWS_SESSION_TOKEN", i.config.SessionToken)
	addEnvIfSet("AWS_PROFILE", i.config.ProfileName)

	// Update the command's environment
	serverCmd.Env = env

	// Set up and start the process with logging
	if err := logger.SetupAndStartProcess(fmt.Sprintf("Java-Iceberg:%d", port), serverCmd); err != nil {
		return fmt.Errorf("failed to start Iceberg server: %s", err)
	}

	// Connect to gRPC server
	conn, err := grpc.NewClient(i.config.ServerHost+`:`+strconv.Itoa(port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	if err != nil {
		// If connection fails, clean up the process
		if serverCmd != nil && serverCmd.Process != nil {
			if killErr := serverCmd.Process.Kill(); killErr != nil {
				logger.Errorf("thread id %s: Failed to kill process: %s", i.threadID, killErr)
			}
		}
		return fmt.Errorf("failed to connect to iceberg writer: %s", err)
	}

	// Register the new server instance
	i.server = &serverInstance{
		port:   port,
		cmd:    serverCmd,
		client: proto.NewRecordIngestServiceClient(conn),
		conn:   conn,
	}

	logger.Infof("thread id %s: Connected to new iceberg writer on port %d", i.threadID, i.server.port)
	return nil
}

// CloseIcebergClient closes the connection to the Iceberg server
func (i *Iceberg) CloseIcebergClient() error {
	// If this was the last reference, shut down the server
	logger.Infof("thread id %s: shutting down Iceberg server on port %d", i.threadID, i.server.port)
	i.server.conn.Close()
	if i.server.cmd != nil && i.server.cmd.Process != nil {
		err := i.server.cmd.Process.Kill()
		if err != nil {
			logger.Errorf("thread id %s: Failed to kill Iceberg server: %s", i.threadID, err)
		}
	}

	return nil
}

// sendRecords sends a slice of records to the Iceberg RPC server
func (i *Iceberg) sendRecords(ctx context.Context, payload types.IcebergWriterPayload) error {
	// Skip if empty
	if len(payload.Records) == 0 {
		return nil
	}

	// // Filter out any empty strings from records
	// validRecords := make([]string, 0, len(payload.Records))
	// for _, record := range payload.Records {
	// 	if record != "" {
	// 		validRecords = append(validRecords, record)
	// 	}
	// }

	// // Skip if all records were empty after filtering
	// if len(validRecords) == 0 {
	// 	return nil
	// }

	// logger.Infof("thread id %s: Sending batch to Iceberg server: %d records", i.threadID, len(validRecords))
	// Create request with all records
	var protoRecords []*proto.IcebergPayload_IceRecord

	schemaFieldsMap := make(map[string]*proto.IcebergPayload_SchemaField)
	for _, record := range payload.Records {
		protoColumns := make(map[string]*structpb.Value)
		for _, iceColumn := range record.Record {
			if iceColumn.Value == nil {
				continue
			}
			// marshal value
			bytesData, err := json.Marshal(iceColumn.Value)
			if err != nil {
				return fmt.Errorf("failed to marshal the value[%v], error: %s", err)
			}
			if iceColumn.Value != nil {
				protoColumns[iceColumn.Key] = structpb.NewStringValue(string(bytesData))
			}
			schemaFieldsMap[iceColumn.Key] = &proto.IcebergPayload_SchemaField{
				IceType: iceColumn.IceType,
				Key:     iceColumn.Key,
			}
		}
		if len(protoColumns) > 0 {
			protoRecords = append(protoRecords, &proto.IcebergPayload_IceRecord{
				Fields:     protoColumns,
				RecordType: record.RecordType,
			})
		}

	}

	var protoSchemaFields []*proto.IcebergPayload_SchemaField
	for _, fields := range schemaFieldsMap {
		protoSchemaFields = append(protoSchemaFields, fields)
	}

	protoMetadata := &proto.IcebergPayload_Metadata{
		DestTableName: payload.Metadata.DestTableName,
		ThreadId:      payload.Metadata.ThreadID,
		PrimaryKey:    &payload.Metadata.PrimaryKey,
		Schema:        protoSchemaFields,
	}
	req := &proto.IcebergPayload{
		Type:     proto.IcebergPayload_RECORDS,
		Metadata: protoMetadata,
		Records:  protoRecords,
	}

	// Send to gRPC server with timeout
	ctx, cancel := context.WithTimeout(ctx, 1000*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := i.server.client.SendRecords(ctx, req)
	if err != nil {
		logger.Errorf("failed to send batch: %s", err)
		return err
	}

	logger.Infof("Sent batch to Iceberg server: %d records, response: %s",
		len(payload.Records),
		res.GetResult())

	return nil
}
