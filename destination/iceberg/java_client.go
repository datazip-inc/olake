package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// defaultServerPort is the port the single shared JVM listens on.
	defaultServerPort = 50051
	// Java class implementing Iceberg's S3FileIOAwsClientFactory
	olakeS3ClientFactoryClass = "io.debezium.server.iceberg.OlakeS3ClientFactory"
)

type serverInstance struct {
	port               int
	cmd                *exec.Cmd
	client             proto.RecordIngestServiceClient
	arrowClient        proto.ArrowIngestServiceClient
	tableIndexClient   proto.TableIndexServiceClient
	conn               *grpc.ClientConn
	defaultServerID    string
	gcpCredentialsTemp string // temp file holding gcp_service_account_json content, if used; cleaned up on Shutdown
}

// getServerConfigJSON builds the catalog/storage-level config the JVM consumes
// at startup. Per-stream concepts (namespace, upsert, identifier-fields,
// partition spec) are deliberately *not* included here — they ride on every
// per-request payload instead. See StreamMetaCtx.
func getServerConfigJSON(config *Config, port int, arrowWriterEnabled bool, gcpCredsTemp string) ([]byte, error) {
	serverConfig := map[string]interface{}{
		"port":                 fmt.Sprintf("%d", port),
		"warehouse":            config.IcebergS3Path,
		"catalog-name":         config.CatalogName,
		"table-prefix":         "",
		"write.format.default": "parquet",
		"arrow-writer-enabled": strconv.FormatBool(arrowWriterEnabled),
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
		addMapKeyIfNotEmpty("gcp.auth.credentials-path", gcpCredsTemp)
		addMapKeyIfNotEmpty("gcp.auth.scopes", config.GCPAuthScopes)
		// BigLake requires this header for request routing/billing.
		addMapKeyIfNotEmpty("header.x-goog-user-project", config.GCPProjectID)
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
		logger.Warnf("No region explicitly provided for Glue catalog, the Java process will attempt to use region from AWS environment")
	}

	if config.S3Endpoint != "" {
		serverConfig["s3.endpoint"] = config.S3Endpoint
	}
	// Some s3-compatible stores (GCS's S3-interop) return 404 when deleting a
	// missing key where AWS returns 204, and Iceberg expects the AWS behavior.
	// The wrapper is a no-op on stores with AWS semantics.
	serverConfig["s3.client-factory-impl"] = olakeS3ClientFactoryClass
	serverConfig["io-impl"] = "org.apache.iceberg.io.ResolvingFileIO"
	serverConfig["s3.ssl-enabled"] = utils.Ternary(config.S3UseSSL, "true", "false").(string)
	// Marshal the config to JSON
	return json.Marshal(serverConfig)
}

// writeGCPCredsTempFile materializes inline gcp_service_account_json content to a
// temp file, since GoogleAuthManager only reads credentials from a file path.
// Returns "" if credsJSON is empty.
func writeGCPCredsTempFile(credsJSON string) (string, error) {
	if credsJSON == "" {
		return "", nil
	}
	f, err := os.CreateTemp("", "olake-gcp-creds-*.json")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file for gcp_service_account_json: %w", err)
	}
	if _, err := f.WriteString(credsJSON); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", fmt.Errorf("failed to write gcp_service_account_json to temp file: %w", err)
	}
	f.Close()
	return f.Name(), nil
}

// startServer launches the JVM and returns the running instance. Invoked once
// from Iceberg.Initialize (via WriterPool.NewWriterPool) before any
// sync/check/clear work begins.
func startServer(config *Config) (*serverInstance, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %w", err)
	}

	const serverID = "shared"
	port := defaultServerPort

	// Forcefully kill any existing process on this port before starting
	reclaimPort(port)

	// GoogleAuthManager only reads credentials from a file path (gcp.auth.credentials-path);
	// materialize inline JSON content to a temp file so the property has something to point at.
	gcpCredsTemp, err := writeGCPCredsTempFile(config.GCPServiceAccountJSON)
	if err != nil {
		return nil, err
	}

	configJSON, err := getServerConfigJSON(config, port, config.UseArrowWrites, gcpCredsTemp)
	if err != nil {
		if gcpCredsTemp != "" {
			os.Remove(gcpCredsTemp)
		}
		return nil, fmt.Errorf("failed to create server config: %w", err)
	}

	// need to do some research on the following flags
	var serverCmd *exec.Cmd
	if os.Getenv("OLAKE_DEBUG_MODE") != "" {
		serverCmd = exec.Command("java",
			"-XX:+UseG1GC",
			"-XX:MaxRAMPercentage=75.0",
			"-XX:+ExitOnOutOfMemoryError",
			"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",
			"-jar", config.JarPath, string(configJSON))
	} else {
		serverCmd = exec.Command("java",
			"-XX:+UseG1GC",
			"-XX:MaxRAMPercentage=75.0",
			"-XX:+ExitOnOutOfMemoryError",
			"-jar", config.JarPath, string(configJSON))
	}

	serverCmd.Env = os.Environ()
	appendEnv := func(key, value string) {
		if value == "" {
			return
		}
		prefix := key + "="
		for i := range serverCmd.Env {
			if strings.HasPrefix(serverCmd.Env[i], prefix) {
				serverCmd.Env[i] = prefix + value
				return
			}
		}
		serverCmd.Env = append(serverCmd.Env, prefix+value)
	}
	appendEnv("AWS_ACCESS_KEY_ID", config.AccessKey)
	appendEnv("AWS_SECRET_ACCESS_KEY", config.SecretKey)
	appendEnv("AWS_REGION", config.Region)
	appendEnv("AWS_SESSION_TOKEN", config.SessionToken)
	appendEnv("AWS_PROFILE", config.ProfileName)
	// GCSFileIO auths separately from GoogleAuthManager; only via ADC env var. No-op if empty.
	appendEnv("GOOGLE_APPLICATION_CREDENTIALS", gcpCredsTemp)

	if err := logger.SetupAndStartProcess(fmt.Sprintf("Iceberg[%d]", port), serverCmd); err != nil {
		if gcpCredsTemp != "" {
			os.Remove(gcpCredsTemp)
		}
		return nil, fmt.Errorf("failed to start iceberg java writer and setup logger: %w", err)
	}

	conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", config.ServerHost, strconv.Itoa(port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		if serverCmd != nil && serverCmd.Process != nil {
			_ = serverCmd.Process.Kill()
		}
		if gcpCredsTemp != "" {
			os.Remove(gcpCredsTemp)
		}
		return nil, fmt.Errorf("failed to create new grpc client: %w", err)
	}

	logger.Infof("Started shared Iceberg JVM on port %d", port)
	return &serverInstance{
		port:               port,
		cmd:                serverCmd,
		client:             proto.NewRecordIngestServiceClient(conn),
		arrowClient:        proto.NewArrowIngestServiceClient(conn),
		tableIndexClient:   proto.NewTableIndexServiceClient(conn),
		conn:               conn,
		defaultServerID:    serverID,
		gcpCredentialsTemp: gcpCredsTemp,
	}, nil
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

// Shutdown kills the JVM and releases its port. Safe to call from defer.
// Signal-driven teardown flows through here too: the root context cancels on
// signal, the command returns, and its deferred Close runs this.
func (s *serverInstance) Shutdown(ctx context.Context) {
	if s == nil {
		return
	}

	logger.Infof("Shutting down shared Iceberg JVM on port %d", s.port)
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.cmd != nil && s.cmd.Process != nil {
		// Ask politely first; the JVM's own shutdown hook releases the gRPC port
		// in an orderly way. Hard-kill only if it doesn't exit in a few seconds.
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{}, 1)
		go func() {
			_, _ = s.cmd.Process.Wait()
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-ctx.Done():
			logger.Warnf("Context canceled, killing Iceberg JVM")
			_ = s.cmd.Process.Kill()
		case <-time.After(10 * time.Second):
			logger.Warnf("Iceberg JVM did not exit within 10s after SIGTERM, killing")
			_ = s.cmd.Process.Kill()
		}
	}
	if s.gcpCredentialsTemp != "" {
		os.Remove(s.gcpCredentialsTemp)
	}
}

// reclaimPort frees the given port by killing whatever process is currently
// bound to it. With a single shared JVM there is no port-pool bookkeeping to
// do — we just make sure the one port we want is available before binding.
func reclaimPort(port int) {
	pid := findProcessUsingPort(port)
	if pid == "" {
		return
	}
	if err := exec.Command("kill", "-9", pid).Run(); err != nil {
		logger.Warnf("Iceberg JVM: failed to kill process %s using port %d: %s", pid, port, err)
		return
	}
	logger.Infof("Iceberg JVM: killed process %s that was using port %d", pid, port)
	// Give the OS a moment to release the socket before we bind to it.
	time.Sleep(2 * time.Second)
}

// findProcessUsingPort finds the PID of a process using the specified port
// Tries ss first (preferred for Alpine), falls back to lsof
func findProcessUsingPort(port int) string {
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
							logger.Infof("Iceberg JVM: found process %s using port %d via ss", pid, port)
							return pid
						}
					}
				}
			}
			// No users: match found; return empty without falling back
			return ""
		}
		// ss failed to run (syntax/permissions/etc.). Log and return empty.
		logger.Warnf("Iceberg JVM: failed to find process using port %d via ss: %s", port, err)
		return ""
	}

	// ss not available: fall back to lsof if present
	if _, lookErr := exec.LookPath("lsof"); lookErr == nil {
		cmd := exec.Command("lsof", "-nP", fmt.Sprintf("-iTCP:%d", port), "-sTCP:LISTEN", "-t")
		output, err := cmd.Output()
		if err == nil {
			pid := strings.TrimSpace(string(output))
			if pid != "" {
				logger.Infof("Iceberg JVM: found process %s using port %d via lsof", pid, port)
				return pid
			}
		}
	}

	return ""
}
