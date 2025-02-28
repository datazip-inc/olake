package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/writers/iceberg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// portMap tracks which ports are in use
var portMap sync.Map

// serverRegistry keeps track of Iceberg server instances to enable reuse
type serverInstance struct {
	port       int
	cmd        *exec.Cmd
	client     proto.StringArrayServiceClient
	conn       *grpc.ClientConn
	refCount   int
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
	batchRegistry   = make(map[string]*recordBatch)
	registryRWMutex sync.RWMutex // Using RWMutex for better read concurrency
	// Maximum batch size before flushing (256MB in bytes)
	maxBatchSize int64 = 256 * 1024 * 1024
	// Local buffer threshold before pushing to shared batch (5MB)
	localBufferThreshold int64 = 5 * 1024 * 1024
	// Thread-local buffer cache using sync.Map to avoid locks
	// Key is configHash + goroutine ID, value is *LocalBuffer
	localBuffers sync.Map
)

// serverRegistry manages active server instances with proper concurrency control
var (
	serverRegistry = make(map[string]*serverInstance)
	registryMutex  sync.Mutex
)

// getConfigHash generates a unique identifier for server configuration per stream
func getConfigHash(config *Config, streamID string, upsert bool) string {
	return fmt.Sprintf("%s-%s-%s-%s-%t", streamID, config.Database, config.S3Path, config.Region, upsert)
}

func findAvailablePort() (int, error) {
	for p := 50051; p <= 59051; p++ {
		// Try to store port in map - returns false if already exists
		if _, loaded := portMap.LoadOrStore(p, true); !loaded {
			// Check if the port is already in use by another process
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", p), time.Second)
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
					logger.Warn("Failed to kill process using port %d: %v", p, err)
					portMap.Delete(p)
					continue
				}

				logger.Info("Killed process %s that was using port %d", pid, p)

				// Wait a moment for the port to be released
				time.Sleep(time.Second * 5)
			}
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available ports found between 50051 and 59051")
}

func (i *Iceberg) SetupIcebergClient(upsert bool) error {
	// Create JSON config for the Java server
	err := i.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// Get the stream ID - if no stream, use a check-specific ID
	var streamID string
	if i.stream == nil {
		// For check operations or when stream isn't available
		streamID = "check_" + utils.ULID() // Generate a unique ID for check operations
	} else {
		streamID = i.stream.ID()
	}

	configHash := getConfigHash(i.config, streamID, upsert)

	// Acquire lock to access registry
	registryMutex.Lock()
	defer registryMutex.Unlock()

	// Check if a server with matching config already exists
	if server, exists := serverRegistry[configHash]; exists {
		// Reuse existing server
		i.port = server.port
		i.client = server.client
		i.conn = server.conn
		i.cmd = server.cmd
		server.refCount++
		logger.Info("Reusing existing Iceberg server", "port", i.port, "stream", streamID, "refCount", server.refCount)
		return nil
	}

	// No matching server found, create a new one
	port, err := findAvailablePort()
	if err != nil {
		return err
	}

	i.port = port

	// Add port to server config
	serverConfig := map[string]string{
		"port":                 fmt.Sprintf("%d", port),
		"catalog-impl":         "org.apache.iceberg.aws.glue.GlueCatalog",
		"io-impl":              "org.apache.iceberg.aws.s3.S3FileIO",
		"warehouse":            i.config.S3Path,
		"table-namespace":      i.config.Database,
		"catalog-name":         "olake_iceberg",
		"glue.region":          i.config.Region,
		"s3.access-key-id":     i.config.AccessKey,
		"s3.secret-access-key": i.config.SecretKey,
		"table-prefix":         "",
		"upsert":               strconv.FormatBool(upsert),
		"upsert-keep-deletes":  "true",
		"write.format.default": "parquet",
		"s3.path-style-access": "true",
	}

	if i.config.SessionToken != "" {
		serverConfig["aws.session-token"] = i.config.SessionToken
	}

	configJSON, err := json.Marshal(serverConfig)
	if err != nil {
		return fmt.Errorf("failed to create server config: %v", err)
	}

	// Start the Java server process
	i.cmd = exec.Command("java", "-jar", i.config.JarPath, string(configJSON))
	i.cmd.Stdout = os.Stdout
	i.cmd.Stderr = os.Stderr

	if err := i.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Iceberg server: %v", err)
	}

	// Connect to gRPC server
	conn, err := grpc.NewClient(`localhost:`+strconv.Itoa(i.port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	if err != nil {
		// If connection fails, clean up the process
		if i.cmd != nil && i.cmd.Process != nil {
			i.cmd.Process.Kill()
		}
		return fmt.Errorf("failed to connect to iceberg writer: %v", err)
	}

	i.conn = conn
	i.client = proto.NewStringArrayServiceClient(conn)

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

	logger.Info("Connected to new iceberg writer", "port", i.port, "stream", streamID, "configHash", configHash)
	return nil
}

func getTestDebeziumRecord() string {
	random_id := utils.ULID()
	return `{
			"destination_table": "olake_test_table",
			"key": {
				"schema" : {
						"type" : "struct",
						"fields" : [ {
							"type" : "string",
							"optional" : true,
							"field" : "_id"
						} ],
						"optional" : false
					},
					"payload" : {
						"_id" : "` + random_id + `"
					}
				}
				,
			"value": {
				"schema" : {
					"type" : "struct",
					"fields" : [ {
					"type" : "string",
					"optional" : true,
					"field" : "_id"
					}, {
					"type" : "boolean",
					"optional" : true,
					"field" : "__deleted"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "__op"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "__db"
					}, {
					"type" : "int64",
					"optional" : true,
					"field" : "__source_ts_ms"
					} ],
					"optional" : false,
					"name" : "dbz_.incr.incr1"
				},
				"payload" : {
					"_id" : "` + random_id + `",
					"__deleted" : false,
					"__op" : "r",
					"__db" : "incr",
					"__source_ts_ms" : 1738502494009
				}
			}
		}`
}

// CloseIcebergClient closes the connection to the Iceberg server
func (i *Iceberg) CloseIcebergClient() error {
	if i.conn == nil {
		return nil // Nothing to close
	}

	registryMutex.Lock()
	defer registryMutex.Unlock()

	// Find the server this instance is using
	var serverKey string
	for key, server := range serverRegistry {
		if server.port == i.port {
			serverKey = key
			break
		}
	}

	if serverKey == "" {
		// Server not found in registry, just clean up the instance
		if i.conn != nil {
			i.conn.Close()
		}

		if i.cmd != nil && i.cmd.Process != nil {
			return i.cmd.Process.Kill()
		}
		return nil
	}

	// Decrement reference count
	server := serverRegistry[serverKey]
	server.refCount--

	// If this was the last reference, shut down the server
	if server.refCount <= 0 {
		logger.Info("Shutting down Iceberg server", "port", i.port)

		// Flush any remaining records before shutting down
		flushBatch(serverKey, server.client)

		if server.conn != nil {
			server.conn.Close()
		}

		if server.cmd != nil && server.cmd.Process != nil {
			err := server.cmd.Process.Kill()
			if err != nil {
				logger.Error("Failed to kill Iceberg server", "error", err)
			}
		}

		// Release the port
		portMap.Delete(i.port)

		// Remove from registry
		delete(serverRegistry, serverKey)

		return nil
	}

	logger.Info("Decreased reference count for Iceberg server", "port", i.port, "refCount", server.refCount)

	// Clear references in this instance but don't close shared resources
	i.conn = nil
	i.cmd = nil
	i.client = nil

	return nil
}

// getOrCreateBatch gets or creates a batch for a specific configuration
func getOrCreateBatch(configHash string) *recordBatch {
	// Try with a read lock first
	registryRWMutex.RLock()
	batch, exists := batchRegistry[configHash]
	registryRWMutex.RUnlock()

	if exists {
		return batch
	}

	// Acquire write lock to create a new batch
	registryRWMutex.Lock()
	defer registryRWMutex.Unlock()

	// Check again in case another goroutine created it
	batch, exists = batchRegistry[configHash]
	if exists {
		return batch
	}

	// Create new batch
	batch = &recordBatch{
		records: make([]string, 0, 1000),
		size:    0,
	}
	batchRegistry[configHash] = batch
	return batch
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
func flushLocalBuffer(buffer *LocalBuffer, configHash string, client proto.StringArrayServiceClient) error {
	if len(buffer.records) == 0 {
		return nil
	}

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
func addToBatch(configHash string, record string, client proto.StringArrayServiceClient) (bool, error) {
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
func flushBatch(configHash string, client proto.StringArrayServiceClient) error {
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
	registryRWMutex.RLock()
	batch, exists := batchRegistry[configHash]
	registryRWMutex.RUnlock()

	if !exists {
		return nil // Nothing to flush
	}

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
func sendRecords(records []string, client proto.StringArrayServiceClient) error {
	// Skip if empty
	if len(records) == 0 {
		return nil
	}

	// Create request with all records
	req := &proto.StringArrayRequest{
		Messages: records,
	}

	// Send to gRPC server with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	// Send the batch to the server
	res, err := client.SendStringArray(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send batch: %v", err)
	}

	logger.Info("Sent batch to Iceberg server",
		"records", len(records),
		"size_mb", float64(len(records)*200)/(1024*1024), // Rough estimate
		"response", res.GetResult())

	return nil
}
