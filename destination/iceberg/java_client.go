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

// defaultServerPort is the single fixed port the Java Iceberg server always runs on.
// Every server start targets this port and kills any process already bound to it,
// so a single writer JVM is used and stale servers from crashed runs are replaced.
const defaultServerPort = 50051

var (
	// sharedMu guards the process-global shared Iceberg server used by all sync
	// writer threads. A single Java process multiplexes every thread (keyed by
	// thread_id on the Java side); only one process is started per sync.
	sharedMu       sync.Mutex
	sharedInstance *serverInstance
)

type serverInstance struct {
	port        int
	cmd         *exec.Cmd
	client      proto.RecordIngestServiceClient
	arrowClient proto.ArrowIngestServiceClient
	conn        *grpc.ClientConn
	serverID    string
	// dedicated is true when this handle owns its own Java process (Check / Clear),
	// false when it is a lightweight handle sharing the process-global server.
	dedicated bool
}

// getServerConfigJSON generates the JSON configuration for the Iceberg server.
// Per-writer settings (namespace, upsert, partition fields) are no longer part of
// launch config; they are carried per request so one process can serve many threads.
func getServerConfigJSON(config *Config, port int, arrowWriterEnabled bool) ([]byte, error) {
	// Create the server configuration map
	serverConfig := map[string]interface{}{
		"port":                     fmt.Sprintf("%d", port),
		"warehouse":                config.IcebergS3Path,
		"catalog-name":             config.CatalogName,
		"table-prefix":             "",
		"create-identifier-fields": !config.NoIdentifierFields,
		"upsert-keep-deletes":      "true",
		"write.format.default":     "parquet",
		"arrow-writer-enabled":     strconv.FormatBool(arrowWriterEnabled),
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

		// if custom glue endpoint creds are passed
		if config.UseGlueAdditionalConfig {
			addMapKeyIfNotEmpty("client.factory", "io.debezium.server.iceberg.OlakeAwsClientFactory")
			addMapKeyIfNotEmpty("glue.access-key-id", config.GlueAccessKey)
			addMapKeyIfNotEmpty("glue.secret-access-key", config.GlueSecretKey)
			addMapKeyIfNotEmpty("glue.endpoint", config.GlueEndpoint)
			addMapKeyIfNotEmpty("glue.id", config.GlueCatalogID)
			addMapKeyIfNotEmpty("glue.region", config.GlueRegion)
		}
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
//
// newIcebergClient returns a handle to an Iceberg gRPC server.
//   - shared=true  (sync write path): all writer threads share ONE process-global
//     Java server; this returns a lightweight handle (own serverID = threadID) that
//     points at the shared connection. The process is started lazily once and torn
//     down by ShutdownSharedServer at sync end.
//   - shared=false (Check / Clear):   a dedicated short-lived process is started and
//     killed when the handle is closed (preserves prior one-off behavior).
func newIcebergClient(config *Config, threadID string, check, shared bool) (*serverInstance, error) {
	// validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %s", err)
	}

	// Using legacy writer java server for Check()
	arrowWriterEnabled := utils.Ternary(check, false, config.UseArrowWrites).(bool)

	if !shared {
		return startServerProcess(config, threadID, arrowWriterEnabled)
	}

	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedInstance == nil {
		inst, err := startServerProcess(config, threadID, arrowWriterEnabled)
		if err != nil {
			return nil, err
		}
		sharedInstance = inst
		logger.Infof("Started shared Iceberg server on port %d (single process for all writer threads)", inst.port)
	}

	// Lightweight handle: shares the process/connection but carries its own
	// thread id, which the Java router uses to isolate per-thread state.
	return &serverInstance{
		port:        sharedInstance.port,
		cmd:         nil,
		client:      sharedInstance.client,
		arrowClient: sharedInstance.arrowClient,
		conn:        sharedInstance.conn,
		serverID:    threadID,
		dedicated:   false,
	}, nil
}

// startServerProcess launches a Java Iceberg server process on the fixed port and
// connects to it. Any process already bound to the port is killed first, so we
// always end up with a fresh single server. The returned instance owns the process
// (dedicated=true).
func startServerProcess(config *Config, threadID string, arrowWriterEnabled bool) (*serverInstance, error) {
	const maxAttempts = 5
	port := defaultServerPort

	addEnvIfSet := func(serverCmd *exec.Cmd, key, value string) {
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

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Always run on the fixed port: kill whatever is currently bound to it
		// (e.g. a stale server from a previous/crashed run) and spawn fresh.
		killProcessOnPort(threadID, port)

		// Build server configuration with the fixed port
		configJSON, err := getServerConfigJSON(config, port, arrowWriterEnabled)
		if err != nil {
			return nil, fmt.Errorf("failed to create server config: %s", err)
		}

		// setup command
		var serverCmd *exec.Cmd
		// If debug mode is enabled
		if os.Getenv("OLAKE_DEBUG_MODE") != "" {
			serverCmd = exec.Command("java", "-XX:+UseG1GC", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005", "-jar", config.JarPath, string(configJSON))
		} else {
			serverCmd = exec.Command("java", "-XX:+UseG1GC", "-jar", config.JarPath, string(configJSON))
		}

		// Get current environment
		serverCmd.Env = os.Environ()
		addEnvIfSet(serverCmd, "AWS_ACCESS_KEY_ID", config.AccessKey)
		addEnvIfSet(serverCmd, "AWS_SECRET_ACCESS_KEY", config.SecretKey)
		addEnvIfSet(serverCmd, "AWS_REGION", config.Region)
		addEnvIfSet(serverCmd, "AWS_SESSION_TOKEN", config.SessionToken)
		addEnvIfSet(serverCmd, "AWS_PROFILE", config.ProfileName)

		// Set up and start the process with logging
		if err := logger.SetupAndStartProcess(fmt.Sprintf("Thread[%s:%d]", threadID, port), serverCmd); err != nil {
			lastErr = err
			// If this was a bind error (EADDRINUSE), the port may still be held by
			// a process we just killed (TIME_WAIT); wait and retry on the same port.
			errLower := strings.ToLower(err.Error())
			if strings.Contains(errLower, "address in use") || strings.Contains(errLower, "failed to bind") || strings.Contains(errLower, "bindexception") || strings.Contains(errLower, "eaddrinuse") {
				logger.Warnf("Thread[%s]: port %d bind failed, killing occupant and retrying", threadID, port)
				time.Sleep(2 * time.Second)
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
			if serverCmd.Process != nil {
				if killErr := serverCmd.Process.Kill(); killErr != nil {
					logger.Errorf("Thread[%s]: Failed to kill process: %s", threadID, killErr)
				}
			}
			return nil, fmt.Errorf("failed to create new grpc client: %s", err)
		}

		logger.Infof("Thread[%s]: Connected to iceberg writer on port %d", threadID, port)
		return &serverInstance{
			port:        port,
			cmd:         serverCmd,
			client:      proto.NewRecordIngestServiceClient(conn),
			arrowClient: proto.NewArrowIngestServiceClient(conn),
			conn:        conn,
			serverID:    threadID,
			dedicated:   true,
		}, nil
	}

	return nil, fmt.Errorf("failed to start iceberg writer on port %d after %d attempts: %v", port, maxAttempts, lastErr)
}

func (s *serverInstance) SendClientRequest(ctx context.Context, payload interface{}) (interface{}, error) {
	switch p := payload.(type) {
	case *proto.IcebergPayload:
		return s.client.SendRecords(ctx, p)
	case *proto.ArrowPayload:
		return s.arrowClient.IcebergAPI(ctx, p)
	default:
		return nil, fmt.Errorf("unsupported payload type: %T", payload)
	}
}

func (s *serverInstance) ServerID() string {
	return s.serverID
}

// closeIcebergClient closes a per-thread handle.
//
// For shared handles (the sync write path) this is a logical close only: the
// process-global server keeps running for other threads and is torn down once by
// ShutdownSharedServer at sync end. For dedicated handles (Check / Clear) the
// owned process and connection are shut down here, as before.
func (s *serverInstance) closeIcebergClient() error {
	if !s.dedicated {
		logger.Debugf("Thread[%s]: logical close (shared Iceberg server retained on port %d)", s.serverID, s.port)
		return nil
	}

	logger.Infof("Thread[%s]: shutting down dedicated Iceberg server on port %d", s.serverID, s.port)
	s.conn.Close()
	if s.cmd != nil && s.cmd.Process != nil {
		err := s.cmd.Process.Kill()
		if err != nil {
			logger.Errorf("Thread[%s]: Failed to kill Iceberg server: %s", s.serverID, err)
		}
	}
	return nil
}

// ShutdownSharedServer stops the process-global Iceberg server, if one was started.
// Safe to call multiple times and when no shared server exists (no-op).
func ShutdownSharedServer() {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedInstance == nil {
		return
	}

	logger.Infof("Shutting down shared Iceberg server on port %d", sharedInstance.port)
	if sharedInstance.conn != nil {
		sharedInstance.conn.Close()
	}
	if sharedInstance.cmd != nil && sharedInstance.cmd.Process != nil {
		if err := sharedInstance.cmd.Process.Kill(); err != nil {
			logger.Errorf("Failed to kill shared Iceberg server: %s", err)
		}
	}
	sharedInstance = nil
}

// killProcessOnPort kills any process currently bound to the given port so a fresh
// server can take it over. No-op if nothing is listening on the port.
func killProcessOnPort(threadID string, port int) {
	pid := findProcessUsingPort(threadID, port)
	if pid == "" {
		return
	}
	if err := exec.Command("kill", "-9", pid).Run(); err != nil {
		logger.Warnf("Thread[%s]: failed to kill process %s on port %d: %s", threadID, pid, port, err)
		return
	}
	logger.Infof("Thread[%s]: killed existing process %s on port %d", threadID, pid, port)
	// Give the OS a moment to release the port before binding again.
	time.Sleep(2 * time.Second)
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
