package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"runtime"
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

// determineMaxBatchSize returns appropriate batch size based on system memory
// This is assuming that each core might create 2 threads, which might eventually need 4 writer threads
func determineMaxBatchSize() int64 {
	ramGB := utils.DetermineSystemMemoryGB()

	var batchSize int64

	switch {
	case ramGB <= 8:
		batchSize = 200 * 1024 * 1024 // 200MB
	case ramGB <= 16:
		batchSize = 400 * 1024 * 1024 // 400MB
	case ramGB <= 32:
		batchSize = 800 * 1024 * 1024 // 800MB
	default:
		batchSize = 1600 * 1024 * 1024 // 1600MB
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

// recordBatch represents a collection of records for a specific server configuration
type recordBatch struct {
	records []string   // Collected Debezium records
	size    int64      // Estimated size in bytes
	mu      sync.Mutex // Mutex for thread-safe access
}

// LocalBuffer represents a thread-local buffer for collecting records
// before adding them to the shared batch
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

// batchRegistry tracks batches of records per server configuration
var (
	// Maximum batch size before flushing (dynamically set based on system memory)
	maxBatchSize = determineMaxBatchSize()
	// Local buffer threshold before pushing to shared batch (5MB)
	localBufferThreshold int64 = 50 * 1024 * 1024
	// Thread-local buffer cache using sync.Map to avoid locks
	// Key is configHash + goroutine ID, value is *LocalBuffer
	localBuffers sync.Map
	// batchRegistry tracks batches of records per server configuration
	// Key is configHash, value is *recordBatch
	batchRegistry sync.Map
)

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
	serverConfig := map[string]string{
		"port":                 fmt.Sprintf("%d", port),
		"warehouse":            i.config.IcebergS3Path,
		"table-namespace":      i.config.IcebergDatabase,
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
		logger.Infof("Reusing existing Iceberg server on port %d for stream %s, refCount %d", i.port, streamID, server.refCount)
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
		return fmt.Errorf("failed to create server config: %v", err)
	}

	// Start the Java server process
	i.cmd = exec.Command("java", "-jar", i.config.JarPath, string(configJSON))

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

	logger.Infof("Connected to new iceberg writer on port %d for stream %s, configHash %s", i.port, streamID, configHash)
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
	// Decrement reference count
	server := serverRegistry[i.configHash]
	server.refCount--

	// If this was the last reference, shut down the server
	if server.refCount <= 0 {
		logger.Infof("Shutting down Iceberg server on port %d", i.port)
		server.conn.Close()

		if server.cmd != nil && server.cmd.Process != nil {
			err := server.cmd.Process.Kill()
			if err != nil {
				logger.Errorf("Failed to kill Iceberg server: %v", err)
			}
		}

		// Release the port
		portMap.Delete(i.port)

		// Remove from registry
		delete(serverRegistry, i.configHash)

		return nil
	}

	logger.Infof("Decreased reference count for Iceberg server on port %d, refCount %d", i.port, server.refCount)

	return nil
}

// getOrCreateBatch gets or creates a batch for a specific configuration
func getOrCreateBatch(configHash string) *recordBatch {
	// LoadOrStore returns the existing value if present, or stores and returns the new value
	batch, _ := batchRegistry.LoadOrStore(configHash, &recordBatch{
		records: make([]string, 0, 1000),
		size:    0,
	})
	return batch.(*recordBatch)
}

// getLocalBuffer gets or creates a local buffer for the current goroutine
func getLocalBuffer(configHash string) *LocalBuffer {
	// Create a unique key for this goroutine + config hash
	bufferID := fmt.Sprintf("%s-%s", configHash, getGoroutineID())

	// Try to get existing buffer
	if val, ok := localBuffers.Load(bufferID); ok {
		return val.(*LocalBuffer)
	}

	// Create new buffer
	buffer := &LocalBuffer{
		records: make([]string, 0, 100),
		size:    0,
	}
	localBuffers.Store(bufferID, buffer)
	return buffer
}

// flushLocalBuffer flushes a local buffer to the shared batch
func flushLocalBuffer(buffer *LocalBuffer, configHash string, client proto.RecordIngestServiceClient) error {
	if len(buffer.records) == 0 {
		return nil
	}

	// TODO: Check if we can remove the below logic as we are locking in java code to iceberg commit already.
	batch := getOrCreateBatch(configHash)

	// Lock the batch only once when flushing the local buffer
	batch.mu.Lock()

	// Add all records from local buffer
	batch.records = append(batch.records, buffer.records...)
	batch.size += buffer.size

	// Check if we need to flush the batch
	needsFlush := batch.size >= maxBatchSize

	var recordsToFlush []string
	if needsFlush {
		// Copy records to flush
		recordsToFlush = batch.records
		// Reset batch
		batch.records = make([]string, 0, 1000)
		batch.size = 0
	}

	// Unlock the batch
	batch.mu.Unlock()

	// Reset local buffer
	buffer.records = make([]string, 0, 100)
	buffer.size = 0

	// Flush if needed
	if needsFlush && len(recordsToFlush) > 0 {
		return sendRecords(recordsToFlush, client)
	}

	return nil
}

// addToBatch adds a record to the batch for a specific server configuration
// Returns true if the batch was flushed, false otherwise
func addToBatch(configHash string, record string, client proto.RecordIngestServiceClient) (bool, error) {
	// If record is empty, skip it
	if record == "" {
		return false, nil
	}

	recordSize := int64(len(record))

	// Get the local buffer for this goroutine
	buffer := getLocalBuffer(configHash)

	// Add record to local buffer (no locking needed as it's per-goroutine)
	buffer.records = append(buffer.records, record)
	buffer.size += recordSize

	// If local buffer is still small, just return
	if buffer.size < localBufferThreshold {
		return false, nil
	}

	// Local buffer reached threshold, flush it to shared batch
	err := flushLocalBuffer(buffer, configHash, client)
	if err != nil {
		return true, err
	}

	return buffer.size >= maxBatchSize, nil
}

// flushBatch forces a flush of all local buffers and the shared batch for a config hash
func flushBatch(configHash string, client proto.RecordIngestServiceClient) error {
	// First, flush all local buffers that match this configHash
	var localBuffersToFlush []*LocalBuffer

	// Collect all local buffers for this config hash
	localBuffers.Range(func(key, value interface{}) bool {
		bufferID := key.(string)
		if strings.HasPrefix(bufferID, configHash+"-") {
			localBuffersToFlush = append(localBuffersToFlush, value.(*LocalBuffer))
		}
		return true
	})

	// Flush each local buffer
	for _, buffer := range localBuffersToFlush {
		err := flushLocalBuffer(buffer, configHash, client)
		if err != nil {
			return err
		}
	}

	// Now check if there's anything in the shared batch
	batchVal, exists := batchRegistry.Load(configHash)
	if !exists {
		return nil // Nothing to flush
	}

	batch := batchVal.(*recordBatch)

	// Lock the batch
	batch.mu.Lock()

	// Skip if batch is empty
	if len(batch.records) == 0 {
		batch.mu.Unlock()
		return nil
	}

	// Copy records to flush
	recordsToFlush := batch.records

	// Reset the batch
	batch.records = make([]string, 0, 1000)
	batch.size = 0

	// Unlock the batch
	batch.mu.Unlock()

	// Send the records
	return sendRecords(recordsToFlush, client)
}

// sendRecords sends a slice of records to the Iceberg RPC server
func sendRecords(records []string, client proto.RecordIngestServiceClient) error {
	// Skip if empty
	if len(records) == 0 {
		return nil
	}

	// Filter out any nil strings from records
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

	// Create request with all records
	req := &proto.RecordIngestRequest{
		Messages: validRecords,
	}

	// Send to gRPC server with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := client.SendRecords(ctx, req)
	if err != nil {
		logger.Errorf("failed to send batch: %v", err)
		return err
	}

	logger.Infof("Sent batch to Iceberg server: %d records, response: %s",
		len(validRecords),
		res.GetResult())

	return nil
}
