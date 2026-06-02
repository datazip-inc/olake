package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	port        int
	cmd         *exec.Cmd
	client      proto.RecordIngestServiceClient
	arrowClient proto.ArrowIngestServiceClient
	conn        *grpc.ClientConn
	serverID    string
}

// Shared single-JVM state for the lifetime of the process.
//
// All Iceberg writers in this OLake process (every stream, every backfill
// chunk, plus the connection Check and DropStreams paths) connect to this one
// JVM instead of forking their own. Per-stream context (namespace, upsert,
// partition spec, identifier-field flag) is carried in each gRPC payload, so
// the JVM stays free of stream-level globals and can serve any combination.
var (
	sharedServerMu    sync.Mutex
	sharedServer      *serverInstance
	shutdownHooksOnce sync.Once
)

// acquireServer returns the shared JVM, lazily starting it on first call.
// The catalog/storage portion of `config` is what drives the JVM CLI; later
// callers that pass a different config still receive the already-running JVM.
// This is intentional: in a single OLake sync the destination config is fixed.
func acquireServer(config *Config) (*serverInstance, error) {
	sharedServerMu.Lock()
	defer sharedServerMu.Unlock()

	if sharedServer != nil {
		return sharedServer, nil
	}

	inst, err := startSharedServer(config)
	if err != nil {
		return nil, err
	}
	sharedServer = inst
	shutdownHooksOnce.Do(installShutdownHooks)
	return sharedServer, nil
}

// shutdownSharedServer kills the JVM and releases its port. Idempotent;
// callers (sync.go defer, Iceberg.Shutdown, signal handler) may race.
func shutdownSharedServer() {
	sharedServerMu.Lock()
	defer sharedServerMu.Unlock()
	if sharedServer == nil {
		return
	}
	inst := sharedServer
	sharedServer = nil

	logger.Infof("Shutting down shared Iceberg JVM on port %d", inst.port)
	if inst.conn != nil {
		_ = inst.conn.Close()
	}
	if inst.cmd != nil && inst.cmd.Process != nil {
		// Ask politely first; the JVM's own shutdown hook releases the gRPC port
		// in an orderly way. Hard-kill only if it doesn't exit in a few seconds.
		_ = inst.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{}, 1)
		go func() {
			_, _ = inst.cmd.Process.Wait()
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logger.Warnf("Iceberg JVM did not exit within 10s after SIGTERM, killing")
			_ = inst.cmd.Process.Kill()
		}
	}
	portStatus.Store(inst.port, &portState{
		inUse:      false,
		releasedAt: time.Now(),
	})
}

// installShutdownHooks ensures the JVM is killed when the Go process is
// signalled (Ctrl-C, kubectl/docker stop, etc). Without this, an orphaned JVM
// can outlive the parent on abrupt termination.
func installShutdownHooks() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		logger.Warnf("Received signal %s, shutting down shared Iceberg JVM", sig)
		shutdownSharedServer()
		// Re-raise the signal so the rest of the process exits normally.
		signal.Reset(syscall.SIGINT, syscall.SIGTERM)
		_ = syscall.Kill(syscall.Getpid(), sig.(syscall.Signal))
	}()
}

func startSharedServer(config *Config) (*serverInstance, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %s", err)
	}

	const maxAttempts = 10
	const serverID = "shared"
	nextStartPort := 50051

	for attempt := 0; attempt < maxAttempts; attempt++ {
		port, err := FindAvailablePort(serverID, nextStartPort)
		if err != nil {
			return nil, fmt.Errorf("failed to find available ports: %s", err)
		}

		configJSON, err := getServerConfigJSON(config, port, config.UseArrowWrites)
		if err != nil {
			return nil, fmt.Errorf("failed to create server config: %s", err)
		}

		var serverCmd *exec.Cmd
		if os.Getenv("OLAKE_DEBUG_MODE") != "" {
			serverCmd = exec.Command("java",
				"-XX:+UseG1GC",
				"-XX:InitialRAMPercentage=40.0",
				"-XX:MaxRAMPercentage=60.0",
				"-XX:MaxDirectMemorySize=8g",
				"-XX:+ExitOnOutOfMemoryError",
				"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",
				"-jar", config.JarPath, string(configJSON))
		} else {
			serverCmd = exec.Command("java",
				"-XX:+UseG1GC",
				"-XX:InitialRAMPercentage=40.0",
				"-XX:MaxRAMPercentage=60.0",
				"-XX:MaxDirectMemorySize=8g",
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

		if err := logger.SetupAndStartProcess(fmt.Sprintf("Iceberg[%d]", port), serverCmd); err != nil {
			portStatus.Store(port, &portState{inUse: false, releasedAt: time.Now()})
			errLower := strings.ToLower(err.Error())
			if strings.Contains(errLower, "address in use") || strings.Contains(errLower, "failed to bind") || strings.Contains(errLower, "bindexception") || strings.Contains(errLower, "eaddrinuse") {
				logger.Warnf("Iceberg JVM: port %d bind failed, retrying with next available port", port)
				nextStartPort = port + 1
				continue
			}
			return nil, fmt.Errorf("failed to start iceberg java writer and setup logger: %s", err)
		}

		conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", config.ServerHost, strconv.Itoa(port)),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
		if err != nil {
			if serverCmd != nil && serverCmd.Process != nil {
				_ = serverCmd.Process.Kill()
			}
			portStatus.Store(port, &portState{inUse: false, releasedAt: time.Now()})
			return nil, fmt.Errorf("failed to create new grpc client: %s", err)
		}

		logger.Infof("Started shared Iceberg JVM on port %d", port)
		return &serverInstance{
			port:        port,
			cmd:         serverCmd,
			client:      proto.NewRecordIngestServiceClient(conn),
			arrowClient: proto.NewArrowIngestServiceClient(conn),
			conn:        conn,
			serverID:    serverID,
		}, nil
	}

	return nil, fmt.Errorf("failed to start iceberg writer after %d attempts due to port binding conflicts", maxAttempts)
}

// getServerConfigJSON builds the catalog/storage-level config the JVM consumes
// at startup. Per-stream concepts (namespace, upsert, identifier-fields,
// partition spec) are deliberately *not* included here — they ride on every
// per-request payload instead. See StreamMetaCtx.
func getServerConfigJSON(config *Config, port int, arrowWriterEnabled bool) ([]byte, error) {
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

	switch config.CatalogType {
	case GlueCatalog:
		serverConfig["catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
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

	serverConfig["s3.path-style-access"] = utils.Ternary(config.S3PathStyle, "true", "false").(string)
	addMapKeyIfNotEmpty("s3.access-key-id", config.AccessKey)
	addMapKeyIfNotEmpty("s3.secret-access-key", config.SecretKey)
	addMapKeyIfNotEmpty("aws.profile", config.ProfileName)
	addMapKeyIfNotEmpty("aws.session-token", config.SessionToken)

	if config.Region != "" {
		serverConfig["s3.region"] = config.Region
	} else if config.S3Endpoint == "" && config.CatalogType == GlueCatalog {
		logger.Warnf("No region explicitly provided for Glue catalog, the Java process will attempt to use region from AWS environment")
	}

	if config.S3Endpoint != "" {
		serverConfig["s3.endpoint"] = config.S3Endpoint
	}
	serverConfig["io-impl"] = "org.apache.iceberg.io.ResolvingFileIO"
	serverConfig["s3.ssl-enabled"] = utils.Ternary(config.S3UseSSL, "true", "false").(string)

	return json.Marshal(serverConfig)
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

// FindAvailablePort finds an available port for the RPC server starting from startPort.
func FindAvailablePort(threadID string, startPort int) (int, error) {
	if startPort < 50051 {
		startPort = 50051
	}
	if startPort > 59051 {
		return 0, fmt.Errorf("startPort out of range")
	}
	for p := startPort; p <= 59051; p++ {
		if state, exists := portStatus.Load(p); exists {
			ps := state.(*portState)
			if ps.inUse {
				continue
			}
			if time.Since(ps.releasedAt) < cooldownPeriod {
				continue
			}
			portStatus.Delete(p)
		}

		if _, loaded := portStatus.LoadOrStore(p, &portState{inUse: true}); !loaded {
			pid := findProcessUsingPort(threadID, p)
			if pid != "" {
				killCmd := exec.Command("kill", "-9", pid)
				killErr := killCmd.Run()
				if killErr == nil {
					logger.Infof("Thread[%s]: Killed process %s that was using port %d", threadID, pid, p)
					time.Sleep(time.Second * 5)
					return p, nil
				}
				logger.Warnf("Thread[%s]: Failed to kill process %s using port %d: %s", threadID, pid, p, killErr)
				portStatus.Store(p, &portState{
					inUse:      false,
					releasedAt: time.Now(),
				})
				continue
			}
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available ports found between 50051 and 59051")
}

// findProcessUsingPort finds the PID of a process using the specified port.
// Tries ss first (preferred for Alpine), falls back to lsof.
func findProcessUsingPort(threadID string, port int) string {
	if _, lookErr := exec.LookPath("ss"); lookErr == nil {
		cmd := exec.Command("ss", "-H", "-ltnp", fmt.Sprintf("sport = :%d", port))
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(output)), "\n")
			for _, line := range lines {
				if strings.Contains(line, "users:") {
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
			return ""
		}
		logger.Warnf("Thread[%s]: Failed to find process using port %d using ss: %s", threadID, port, err)
		return ""
	}

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
