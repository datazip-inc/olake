/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// determineMaxBatchSize returns appropriate batch size based on system memory
// This is assuming that each core might create 2 threads, which might eventually need 4 writer threads
func determineMaxBatchSize() int64 {
	ramGB := utils.DetermineSystemMemoryGB()

	var batchSize int64

	switch {
	case ramGB <= 16:
		batchSize = 100 * 1024 * 1024 // 100MB
	case ramGB <= 32:
		batchSize = 400 * 1024 * 1024 // 400MB
	default:
		batchSize = 800 * 1024 * 1024 // 800MB
	}

	logger.Infof("System has %dGB RAM, setting iceberg writer batch size to %d bytes", ramGB, batchSize)
	return batchSize
}

// portMap tracks which ports are in use
var portMap sync.Map

// serverRegistry keeps track of Iceberg server instances to enable reuse
type serverInstance struct {
	port       int
	cmd        *exec.Cmd
	client     proto.RecordIngestServiceClient
	conn       *grpc.ClientConn
	refCount   int    // Tracks how many clients are using this server instance to manage shared resources. Comes handy when we need to close the server after all clients are done.
	configHash string // Hash representing the server config
	upsert     bool
	streamID   string // Store the stream ID
}

// LocalBuffer represents a thread-local buffer for collecting records
// before sending them directly to the server
type LocalBuffer struct {
	records []string
	size    int64
}

// getGoroutineID returns a unique ID for the current goroutine
// This is a simple implementation that uses the string address of a local variable
// which will be unique per goroutine
func getGoroutineID() string {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	id := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	return id
}

// Maximum batch size before flushing (dynamically set based on system memory)
var maxBatchSize = determineMaxBatchSize()

// serverRegistry manages active server instances with proper concurrency control
var (
	serverRegistry = make(map[string]*serverInstance)
)

// getConfigHash generates a unique identifier for server configuration per stream
func getConfigHash(namespace string, streamID string, upsert bool) string {
	hashComponents := []string{
		streamID,
		namespace,
		fmt.Sprintf("%t", upsert),
	}
	return strings.Join(hashComponents, "-")
}

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

// parsePartitionRegex parses the partition regex and populates the partitionInfo slice
func (i *Iceberg) parsePartitionRegex(pattern string) error {
	// path pattern example: /{col_name, partition_transform}/{col_name, partition_transform}
	// This strictly identifies column name and partition transform entries
	patternRegex := regexp.MustCompile(constants.PartitionRegexIceberg)
	matches := patternRegex.FindAllStringSubmatch(pattern, -1)
	for _, match := range matches {
		if len(match) < 3 {
			continue // We need at least 3 matches: full match, column name, transform
		}

		colName := strings.Replace(strings.TrimSpace(strings.Trim(match[1], `'"`)), "now()", constants.OlakeTimestamp, 1)
		transform := strings.TrimSpace(strings.Trim(match[2], `'"`))

		// Append to ordered slice to preserve partition order
		i.partitionInfo = append(i.partitionInfo, PartitionInfo{
			Field:     colName,
			Transform: transform,
		})
	}

	return nil
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
		if i.config.RestSigningName != "" {
			serverConfig["rest.signing-name"] = i.config.RestSigningName
		}
		if i.config.RestSigningRegion != "" {
			serverConfig["rest.signing-region"] = i.config.RestSigningRegion
		}
		serverConfig["rest.sigv4-enabled"] = strconv.FormatBool(i.config.RestSigningV4)
		if i.config.RestToken != "" {
			serverConfig["token"] = i.config.RestToken
		}
		if i.config.RestOAuthURI != "" {
			serverConfig["oauth2-server-uri"] = i.config.RestOAuthURI
		}
		if i.config.RestAuthType != "" {
			serverConfig["rest.auth.type"] = i.config.RestAuthType
		}
		if i.config.RestCredential != "" {
			serverConfig["credential"] = i.config.RestCredential
		}
		if i.config.RestScope != "" {
			serverConfig["scope"] = i.config.RestScope
		}

	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", i.config.CatalogType)
	}

	// Configure S3 or GCP file IO
	serverConfig["io-impl"] = utils.Ternary(strings.HasPrefix(i.config.IcebergS3Path, "gs://"), "org.apache.iceberg.gcp.gcs.GCSFileIO", "org.apache.iceberg.aws.s3.S3FileIO")

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

// Initialize the local buffer in the Iceberg instance during setup
func (i *Iceberg) SetupIcebergClient(upsert bool) error {
	// Create JSON config for the Java server
	err := i.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	var streamID, namespace string
	if i.stream == nil {
		// For check operations or when stream isn't available
		streamID, namespace = "check_"+utils.ULID(), "check"
	} else {
		streamID, namespace = i.stream.ID(), i.stream.Namespace()
	}
	configHash := getConfigHash(namespace, streamID, upsert)
	i.configHash = configHash

	// Check if a server with matching config already exists
	if server, exists := serverRegistry[configHash]; exists {
		// Reuse existing server
		i.port, i.client, i.conn, i.cmd = server.port, server.client, server.conn, server.cmd
		server.refCount++
		logger.Infof("thread id %s: reusing existing Iceberg server on port %d for stream %s, refCount %d", i.threadID, i.port, streamID, server.refCount)
		return nil
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
		return fmt.Errorf("failed to create server config: %s", err)
	}

	// Start the Java server process
	// If debug mode is enabled and stream is available (stream is nil for check operations), start the server with debug options
	if os.Getenv("OLAKE_DEBUG_MODE") != "" && i.stream != nil {
		i.cmd = exec.Command("java", "-XX:+UseG1GC", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005", "-jar", i.config.JarPath, string(configJSON))
	} else {
		i.cmd = exec.Command("java", "-XX:+UseG1GC", "-jar", i.config.JarPath, string(configJSON))
	}

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
		return fmt.Errorf("failed to start Iceberg server: %s", err)
	}

	// Connect to gRPC server
	conn, err := grpc.NewClient(i.config.ServerHost+`:`+strconv.Itoa(port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	if err != nil {
		// If connection fails, clean up the process
		if i.cmd != nil && i.cmd.Process != nil {
			if killErr := i.cmd.Process.Kill(); killErr != nil {
				logger.Errorf("thread id %s: Failed to kill process: %s", i.threadID, killErr)
			}
		}
		return fmt.Errorf("failed to connect to iceberg writer: %s", err)
	}

	i.port, i.conn, i.client = port, conn, proto.NewRecordIngestServiceClient(conn)

	// Register the new server instance
	serverRegistry[configHash] = &serverInstance{
		port:       i.port,
		cmd:        i.cmd,
		client:     i.client,
		conn:       i.conn,
		refCount:   1,
		configHash: configHash,
		upsert:     upsert,
		streamID:   streamID,
	}

	logger.Infof("thread id %s: Connected to new iceberg writer on port %d for stream %s, configHash %s", i.threadID, i.port, streamID, configHash)
	return nil
}

func getTestDebeziumRecord(threadID string) string {
	randomID := utils.ULID()

	return `{
			"destination_table": "olake_test_table",
			"thread_id": "` + threadID + `",
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
					"type" : "timestamptz",
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
	// Decrement reference count
	server := serverRegistry[i.configHash]
	server.refCount--

	// If this was the last reference, shut down the server
	if server.refCount <= 0 {
		logger.Infof("thread id %s: shutting down Iceberg server on port %d", i.threadID, i.port)
		server.conn.Close()

		if server.cmd != nil && server.cmd.Process != nil {
			err := server.cmd.Process.Kill()
			if err != nil {
				logger.Errorf("thread id %s: Failed to kill Iceberg server: %s", i.threadID, err)
			}
		}

		// Release the port
		portMap.Delete(i.port)

		// Remove from registry
		delete(serverRegistry, i.configHash)

		return nil
	}

	logger.Infof("thread id %s: decreased reference count for Iceberg server on port %d, refCount %d", i.threadID, i.port, server.refCount)

	return nil
}

// flushLocalBuffer flushes a local buffer directly to the server
func (i *Iceberg) flushLocalBuffer(ctx context.Context, buffer *LocalBuffer) error {
	// Send records directly to server
	if len(buffer.records) > 0 {
		err := i.sendRecords(ctx, buffer.records)
		if err != nil {
			return err
		}
	}

	buffer.records = make([]string, 0, 100)
	buffer.size = 0

	return nil
}

// sendRecords sends a slice of records to the Iceberg RPC server
func (i *Iceberg) sendRecords(ctx context.Context, records []string) error {
	// Skip if empty
	if len(records) == 0 {
		return nil
	}

	// Filter out any empty strings from records
	validRecords := make([]string, 0, len(records))
	for _, record := range records {
		if record != "" {
			validRecords = append(validRecords, record)
		}
	}

	// Skip if all records were empty after filtering
	if len(validRecords) == 0 {
		return nil
	}

	logger.Infof("thread id %s: Sending batch to Iceberg server: %d records", i.threadID, len(validRecords))
	// Create request with all records
	req := &proto.RecordIngestRequest{
		Messages: validRecords,
	}

	// Send to gRPC server with timeout
	ctx, cancel := context.WithTimeout(ctx, 1000*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := i.client.SendRecords(ctx, req)
	if err != nil {
		logger.Errorf("failed to send batch: %s", err)
		return err
	}

	logger.Infof("Sent batch to Iceberg server: %d records, response: %s",
		len(validRecords),
		res.GetResult())

	return nil
}
