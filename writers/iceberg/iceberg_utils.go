package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/writers/iceberg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// portMap tracks which ports are in use
var portMap sync.Map

// findAvailablePort finds an available port for the RPC server
func findAvailablePort(serverHost string) (int, error) {
	for p := 50051; p <= 59051; p++ {
		// Try to store port in map - returns false if already exists
		if _, loaded := portMap.LoadOrStore(p, true); !loaded {
			// Check if the port is already in use by another process
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", serverHost, p), time.Second)
			if err == nil {
				// Port is in use, close our test connection
				conn.Close()

				// Find the process using this port
				cmd := exec.Command("lsof", "-i", fmt.Sprintf(":%d", p), "-t")
				output, err := cmd.Output()
				if err != nil {
					// Failed to find process, continue to next port
					portMap.Delete(p)
					continue
				}

				// Get the PID
				pid := strings.TrimSpace(string(output))
				if pid == "" {
					// No process found, continue to next port
					portMap.Delete(p)
					continue
				}

				// Kill the process
				killCmd := exec.Command("kill", "-9", pid)
				err = killCmd.Run()
				if err != nil {
					logger.Warnf("Failed to kill process using port %d: %v", p, err)
					portMap.Delete(p)
					continue
				}

				logger.Infof("Killed process %s that was using port %d", pid, p)

				// Wait a moment for the port to be released
				time.Sleep(time.Second * 5)
			}
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available ports found between 50051 and 59051")
}

// parsePartitionRegex parses the partition regex and populates the partitionInfo map
func (i *Iceberg) parsePartitionRegex(pattern string) error {
	// path pattern example: /{col_name, partition_transform}/{col_name, partition_transform}
	// This strictly identifies column name and partition transform entries
	patternRegex := regexp.MustCompile(`\{([^,]+),\s*([^}]+)\}`)
	matches := patternRegex.FindAllStringSubmatch(pattern, -1)
	for _, match := range matches {
		if len(match) < 3 {
			continue // We need at least 3 matches: full match, column name, transform
		}

		colName := strings.Replace(strings.TrimSpace(strings.Trim(match[1], `'"`)), "now()", constants.OlakeTimestamp, 1)
		transform := strings.TrimSpace(strings.Trim(match[2], `'"`))

		// Store transform for this field
		i.partitionInfo[colName] = transform
	}

	return nil
}

// getServerConfigJSON generates the JSON configuration for the Iceberg server
func (i *Iceberg) getServerConfigJSON(port int, upsert bool) ([]byte, error) {
	// Create the server configuration map
	streamName := "olake_test_table"
	if i.stream != nil {
		streamName = i.stream.Name()
	}
	serverConfig := map[string]string{
		"port":                 fmt.Sprintf("%d", port),
		"warehouse":            i.config.IcebergS3Path,
		"table-namespace":      i.config.IcebergDatabase,
		"table-name":           streamName,
		"catalog-name":         "olake_iceberg",
		"table-prefix":         "",
		"upsert":               strconv.FormatBool(upsert),
		"upsert-keep-deletes":  "true",
		"write.format.default": "parquet",
	}

	// Add partition fields if defined
	for field, transform := range i.partitionInfo {
		partitionKey := fmt.Sprintf("partition.field.%s", field)
		serverConfig[partitionKey] = transform
	}

	// Configure catalog implementation based on the selected type
	switch i.config.CatalogType {
	case GlueCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
	case JDBCCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.jdbc.JdbcCatalog"
		serverConfig["uri"] = i.config.JDBCUrl
		if i.config.JDBCUsername != "" {
			serverConfig["jdbc.user"] = i.config.JDBCUsername
		}
		if i.config.JDBCPassword != "" {
			serverConfig["jdbc.password"] = i.config.JDBCPassword
		}
	case HiveCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.hive.HiveCatalog"
		serverConfig["uri"] = i.config.HiveURI
		serverConfig["clients"] = strconv.Itoa(i.config.HiveClients)
		serverConfig["hive.metastore.sasl.enabled"] = strconv.FormatBool(i.config.HiveSaslEnabled)
		serverConfig["engine.hive.enabled"] = "true"
	case RestCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.rest.RESTCatalog"
		serverConfig["uri"] = i.config.RestCatalogURL

	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", i.config.CatalogType)
	}

	// Configure S3 file IO
	serverConfig["io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"

	// Only set access keys if explicitly provided, otherwise they'll be picked up from
	// environment variables or AWS credential files
	if i.config.AccessKey != "" {
		serverConfig["s3.access-key-id"] = i.config.AccessKey
	}
	if i.config.SecretKey != "" {
		serverConfig["s3.secret-access-key"] = i.config.SecretKey
	}
	// If profile is specified, add it to the config
	if i.config.ProfileName != "" {
		serverConfig["aws.profile"] = i.config.ProfileName
	}

	// Use path-style access by default for S3-compatible services
	if i.config.S3PathStyle {
		serverConfig["s3.path-style-access"] = "true"
	} else {
		serverConfig["s3.path-style-access"] = "false"
	}

	// Add AWS session token if provided
	if i.config.SessionToken != "" {
		serverConfig["aws.session-token"] = i.config.SessionToken
	}

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
		if i.config.S3UseSSL {
			serverConfig["s3.ssl-enabled"] = "true"
		} else {
			serverConfig["s3.ssl-enabled"] = "false"
		}
	}

	// Marshal the config to JSON
	return json.Marshal(serverConfig)
}

func (i *Iceberg) SetupIcebergClient(upsert bool) error {
	// Create JSON config for the Java server
	err := i.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// No matching server found, create a new one
	port, err := findAvailablePort(i.config.ServerHost)
	if err != nil {
		return err
	}

	i.port = port

	// Get the server configuration JSON
	configJSON, err := i.getServerConfigJSON(port, upsert)
	if err != nil {
		return fmt.Errorf("failed to create server config: %v", err)
	}

	// Start the Java server process
	// If debug mode is enabled and stream is available (stream is nil for check operations), start the server with debug options
	i.cmd = utils.Ternary(i.config.DebugMode && i.stream != nil, exec.Command("java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005", "-jar", i.config.JarPath, string(configJSON)), exec.Command("java", "-jar", i.config.JarPath, string(configJSON))).(*exec.Cmd)

	// Set environment variables for AWS credentials and region when using Glue catalog
	// Get current environment
	env := i.cmd.Env
	if env == nil {
		env = []string{}
	}

	// Add AWS credentials and region as environment variables if provided
	if i.config.AccessKey != "" {
		env = append(env, "AWS_ACCESS_KEY_ID="+i.config.AccessKey)
	}
	if i.config.SecretKey != "" {
		env = append(env, "AWS_SECRET_ACCESS_KEY="+i.config.SecretKey)
	}
	if i.config.Region != "" {
		env = append(env, "AWS_REGION="+i.config.Region)
	}
	if i.config.SessionToken != "" {
		env = append(env, "AWS_SESSION_TOKEN="+i.config.SessionToken)
	}
	if i.config.ProfileName != "" {
		env = append(env, "AWS_PROFILE="+i.config.ProfileName)
	}

	// Update the command's environment
	i.cmd.Env = env

	// Set up and start the process with logging
	processName := fmt.Sprintf("Java-Iceberg:%d", port)
	if err := logger.SetupAndStartProcess(processName, i.cmd); err != nil {
		return fmt.Errorf("failed to start Iceberg server: %v", err)
	}

	// Connect to gRPC server
	conn, err := grpc.NewClient(i.config.ServerHost+`:`+strconv.Itoa(port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	if err != nil {
		// If connection fails, clean up the process
		if i.cmd != nil && i.cmd.Process != nil {
			if killErr := i.cmd.Process.Kill(); killErr != nil {
				logger.Errorf("Failed to kill process: %v", killErr)
			}
		}
		return fmt.Errorf("failed to connect to iceberg writer: %v", err)
	}

	i.port, i.conn, i.client = port, conn, proto.NewRecordIngestServiceClient(conn)
	return nil
}

func getTestDebeziumRecord() string {
	randomID := utils.ULID()
	return `{
			"destination_table": "olake_test_table",
			"key": {
				"schema" : {
						"type" : "struct",
						"fields" : [ {
							"type" : "string",
							"optional" : true,
							"field" : "` + constants.OlakeID + `"
						} ],
						"optional" : false
					},
					"payload" : {
						"` + constants.OlakeID + `" : "` + randomID + `"
					}
				}
				,
			"value": {
				"schema" : {
					"type" : "struct",
					"fields" : [ {
					"type" : "string",
					"optional" : true,
					"field" : "` + constants.OlakeID + `"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "` + constants.OpType + `"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "` + constants.DBName + `"
					}, {
					"type" : "int64",
					"optional" : true,
					"field" : "` + constants.OlakeTimestamp + `"
					} ],
					"optional" : false,
					"name" : "dbz_.incr.incr1"
				},
				"payload" : {
					"` + constants.OlakeID + `" : "` + randomID + `",
					"` + constants.OpType + `" : "r",
					"` + constants.DBName + `" : "incr",
					"` + constants.OlakeTimestamp + `" : 1738502494009
				}
			}
		}`
}

// CloseIcebergClient closes the connection to the Iceberg server
func (i *Iceberg) CloseIcebergClient() error {
	// If this was the last reference, shut down the server
	if i.records.Load() > 0 {
		err := sendRecord(`{"commit": true}`, i.client)
		if err != nil {
			return fmt.Errorf("failed to add record to batch: %v", err)
		}
	}
	i.conn.Close()

	if i.cmd != nil && i.cmd.Process != nil {
		err := i.cmd.Process.Kill()
		if err != nil {
			logger.Errorf("Failed to kill Iceberg server: %v", err)
		}
	}

	// Release the port
	portMap.Delete(i.port)
	return nil
}

// sendRecord sends a record to the Iceberg RPC server
func sendRecord(record string, client proto.RecordIngestServiceClient) error {
	// Skip if empty
	if record == "" {
		return nil
	}

	// Create request with all records
	req := &proto.RecordIngestRequest{
		Messages: []string{record},
	}

	// Send to gRPC server with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	// Send the batch to the server
	_, err := client.SendRecords(ctx, req)
	if err != nil {
		logger.Errorf("failed to send batch: %v", err)
		return err
	}

	return nil
}
