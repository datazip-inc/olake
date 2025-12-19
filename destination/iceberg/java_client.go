package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	portStatus     sync.Map // map[int]*portState - tracks port usage and cooldown state
	cooldownPeriod = 180 * time.Second
)

type portState struct {
	inUse      bool
	releasedAt time.Time
}

type serverInstance struct {
	port     int
	cmd      *exec.Cmd
	client   proto.RecordIngestServiceClient
	conn     *grpc.ClientConn
	serverID string
}

// getServerConfigJSON generates the JSON configuration for the Iceberg server
func getServerConfigJSON(config *Config, partitionInfo []PartitionInfo, port int, upsert bool, destinationDatabase string) ([]byte, error) {
	// Create the server configuration map
	serverConfig := map[string]interface{}{
		"port":                     fmt.Sprintf("%d", port),
		"warehouse":                config.IcebergS3Path,
		"table-namespace":          destinationDatabase,
		"catalog-name":             config.CatalogName,
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
		logger.Warnf("No region explicitly provided for Glue catalog, the Java process will attempt to use region from AWS environment")
	}

	if config.S3Endpoint != "" {
		serverConfig["s3.endpoint"] = config.S3Endpoint
	}
	serverConfig["io-impl"] = "org.apache.iceberg.io.ResolvingFileIO"
	serverConfig["s3.ssl-enabled"] = utils.Ternary(config.S3UseSSL, "true", "false").(string)

	// Marshal the config to JSON
	return json.Marshal(serverConfig)
}

// setup java client

func newIcebergClient(config *Config, partitionInfo []PartitionInfo, threadID string, check, upsert bool, destinationDatabase string) (*serverInstance, error) {
	// validate configuration
	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("failed to validate config: %s", err)
	}

	const maxAttempts = 10
	var (
		port      int
		serverCmd *exec.Cmd
	)

	// nextStartPort controls from where the port scan should begin on each attempt
	nextStartPort := 50051

	addEnvIfSet := func(key, value string) {
		if value != "" {
			keyPrefix := fmt.Sprintf("%s=", key)
			for idx := range serverCmd.Env {
				// if prefix exist through env, override it with config
				if strings.HasPrefix(serverCmd.Env[idx], keyPrefix) {
					serverCmd.Env[idx] = fmt.Sprintf("%s=%s", key, value)
					return
				}
			}
			// if prefix does not exist add it
			serverCmd.Env = append(serverCmd.Env, fmt.Sprintf("%s=%s", key, value))
		}
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// get available port
		port, err = FindAvailablePort(threadID, nextStartPort)
		if err != nil {
			return nil, fmt.Errorf("failed to find available ports: %s", err)
		}

		// Build server configuration with selected port
		configJSON, err := getServerConfigJSON(config, partitionInfo, port, upsert, destinationDatabase)
		if err != nil {
			return nil, fmt.Errorf("failed to create server config: %s", err)
		}

		// setup command
		// If debug mode is enabled and it is not check command
		if os.Getenv("OLAKE_DEBUG_MODE") != "" && !check {
			serverCmd = exec.Command("java", "-XX:+UseG1GC", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005", "-jar", config.JarPath, string(configJSON))
		} else {
			serverCmd = exec.Command("java", "-XX:+UseG1GC", "-jar", config.JarPath, string(configJSON))
		}

		// Get current environment
		serverCmd.Env = os.Environ()
		addEnvIfSet("AWS_ACCESS_KEY_ID", config.AccessKey)
		addEnvIfSet("AWS_SECRET_ACCESS_KEY", config.SecretKey)
		addEnvIfSet("AWS_REGION", config.Region)
		addEnvIfSet("AWS_SESSION_TOKEN", config.SessionToken)
		addEnvIfSet("AWS_PROFILE", config.ProfileName)

		// Set up and start the process with logging
		if err := logger.SetupAndStartProcess(fmt.Sprintf("Thread[%s:%d]", threadID, port), serverCmd); err != nil {
			// Mark port for cooldown since it failed to start
			portStatus.Store(port, &portState{
				inUse:      false,
				releasedAt: time.Now(),
			})
			// If this was a bind error (EADDRINUSE), retry with the next available port
			// This is necessary because port can collide with system ephemeral ports, which we can not detect/kill, so we skip
			errLower := strings.ToLower(err.Error())
			if strings.Contains(errLower, "address in use") || strings.Contains(errLower, "failed to bind") || strings.Contains(errLower, "bindexception") || strings.Contains(errLower, "eaddrinuse") {
				logger.Warnf("Thread[%s]: Port %d bind failed, retrying with next available port", threadID, port)
				// advance the start port to the next one for the subsequent scan
				nextStartPort = port + 1
				continue
			}
			return nil, fmt.Errorf("failed to start iceberg java writer and setup logger: %s", err)
		}

		// Connect to gRPC server
		conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", config.ServerHost, strconv.Itoa(port)),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

		if err != nil {
			// If connection fails, clean up the process
			if serverCmd != nil && serverCmd.Process != nil {
				if killErr := serverCmd.Process.Kill(); killErr != nil {
					logger.Errorf("Thread[%s]: Failed to kill process: %s", threadID, killErr)
				}
			}
			// Mark port for cooldown since connection failed
			portStatus.Store(port, &portState{
				inUse:      false,
				releasedAt: time.Now(),
			})
			return nil, fmt.Errorf("failed to create new grpc client: %s", err)
		}

		logger.Infof("Thread[%s]: Connected to new iceberg writer on port %d", threadID, port)
		return &serverInstance{
			port:     port,
			cmd:      serverCmd,
			client:   proto.NewRecordIngestServiceClient(conn),
			conn:     conn,
			serverID: threadID,
		}, nil
	}

	return nil, fmt.Errorf("failed to start iceberg writer after %d attempts due to port binding conflicts", maxAttempts)
}

func (s *serverInstance) sendClientRequest(ctx context.Context, reqPayload *proto.IcebergPayload) (string, error) {
	resp, err := s.client.SendRecords(ctx, reqPayload)
	if err != nil {
		return "", fmt.Errorf("failed to send grpc request: %s", err)
	}
	return resp.GetResult(), nil
}

// closeIcebergClient closes the connection to the Iceberg server
func (s *serverInstance) closeIcebergClient() error {
	// If this was the last reference, shut down the server
	logger.Infof("Thread[%s]: shutting down Iceberg server on port %d", s.serverID, s.port)
	s.conn.Close()
	if s.cmd != nil && s.cmd.Process != nil {
		err := s.cmd.Process.Kill()
		if err != nil {
			logger.Errorf("Thread[%s]: Failed to kill Iceberg server: %s", s.serverID, err)
		}
	}
	// Mark port as released (cooldown period to allow OS to clean up TIME_WAIT state)
	portStatus.Store(s.port, &portState{
		inUse:      false,
		releasedAt: time.Now(),
	})
	return nil
}

// findAvailablePort finds an available port for the RPC server starting from startPort
func FindAvailablePort(threadID string, startPort int) (int, error) {
	if startPort < 50051 {
		startPort = 50051
	}
	if startPort > 59051 {
		return 0, fmt.Errorf("startPort out of range")
	}
	for p := startPort; p <= 59051; p++ {
		// Check port state
		if state, exists := portStatus.Load(p); exists {
			ps := state.(*portState)
			if ps.inUse {
				// Port is currently in use by our process
				continue
			}
			// Port was released, check if cooldown period has elapsed
			if time.Since(ps.releasedAt) < cooldownPeriod {
				// Still in cooldown, skip this port
				continue
			}
			// Cooldown period expired, remove from map and allow reuse
			portStatus.Delete(p)
		}

		// Port not tracked or cooldown expired - try to acquire it
		// Use LoadOrStore to atomically claim the port
		if _, loaded := portStatus.LoadOrStore(p, &portState{inUse: true}); !loaded {
			// Successfully claimed the port, try to kill any process using it
			pid := findProcessUsingPort(threadID, p)
			if pid != "" {
				// Kill the process
				killCmd := exec.Command("kill", "-9", pid)
				killErr := killCmd.Run()
				if killErr == nil {
					logger.Infof("Thread[%s]: Killed process %s that was using port %d", threadID, pid, p)
					// Wait for the port to be released
					time.Sleep(time.Second * 5)
					return p, nil
				}
				logger.Warnf("Thread[%s]: Failed to kill process %s using port %d: %s", threadID, pid, p, killErr)
				// Release the claim and mark cooldown, then continue scanning
				portStatus.Store(p, &portState{
					inUse:      false,
					releasedAt: time.Now(),
				})
				continue
			}
			// Return the port (either it was free, or we attempted to free it)
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available ports found between 50051 and 59051")
}

// findProcessUsingPort finds the PID of a process using the specified port
// Tries ss first (preferred for Alpine), falls back to lsof
func findProcessUsingPort(threadID string, port int) string {
	// Prefer ss if available. If ss exists, do NOT fall back to lsof.
	if _, lookErr := exec.LookPath("ss"); lookErr == nil {
		// Use a valid filter expression: sport = :<port>
		cmd := exec.Command("ss", "-H", "-ltnp", fmt.Sprintf("sport = :%d", port))
		output, err := cmd.Output()
		if err == nil {
			// Parse ss output to extract PID
			lines := strings.Split(strings.TrimSpace(string(output)), "\n")
			for _, line := range lines {
				// ss output format: State Recv-Q Send-Q Local Address:Port Peer Address:Port Process
				// Look for the process part at the end (e.g., "users:((\"java\",pid=123,fd=123))")
				if strings.Contains(line, "users:") {
					// Extract PID from the process info
					parts := strings.Split(line, "pid=")
					if len(parts) > 1 {
						pidPart := strings.Split(parts[1], ",")[0]
						if pid := strings.TrimSpace(pidPart); pid != "" {
							logger.Infof("Thread[%s]: Found process %s using port %d using ss", threadID, pid, port)
							return pid
						}
					}
				}
			}
			// No users: match found; return empty without falling back
			return ""
		}
		// ss failed to run (syntax/permissions/etc.). Log and return empty.
		logger.Warnf("Thread[%s]: Failed to find process using port %d using ss: %s", threadID, port, err)
		return ""
	}

	// ss not available: fall back to lsof if present
	if _, lookErr := exec.LookPath("lsof"); lookErr == nil {
		cmd := exec.Command("lsof", "-nP", fmt.Sprintf("-iTCP:%d", port), "-sTCP:LISTEN", "-t")
		output, err := cmd.Output()
		if err == nil {
			pid := strings.TrimSpace(string(output))
			if pid != "" {
				logger.Infof("Thread[%s]: Found process %s using port %d using lsof", threadID, pid, port)
				return pid
			}
		}
	}

	return ""
}
