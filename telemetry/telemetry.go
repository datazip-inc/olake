package telemetry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	analytics "github.com/segmentio/analytics-go/v3"
	"github.com/spf13/viper"
)

var (
	client           analytics.Client
	once             sync.Once
	instance         *Telemetry
	idLock           sync.Mutex
	telemetryEnabled bool
	deploymentType   string
	serviceName      string
	segmentAPIKey    string
)

const (
	anonymousIDFile = "telemetry_id"
	version         = "1.0.0" // Should be set during build
)

type Telemetry struct {
	client      analytics.Client
	serviceName string
	enabled     bool
	platform    platformInfo
}

type platformInfo struct {
	OS           string
	Arch         string
	OlakeVersion string
}

func loadConfig() {
	viper.SetConfigName("config-telemetry")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("../../") // current directory

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		return
	}

	telemetryEnabled = viper.GetBool("telemetry.enabled")
	segmentAPIKey = viper.GetString("telemetry.segment_api_key")
	deploymentType = viper.GetString("telemetry.deployment_type")
	serviceName = viper.GetString("telemetry.service_name")
}

// var telemetryEnabled = true
// var deploymentType = "development"
// var serviceName = "olake"
// var segmentAPIKey = "RFynIrEFUsaTRgKumlSqfme6DRPymYfN"

func createTelemetry() {
	loadConfig()
	// Initialize only if telemetry is enabled
	if isTelemetryEnabled() {
		fmt.Println("Telemetry is enabled")
		fmt.Println("Segment Key:", segmentAPIKey)

		// Create client with proper configuration for more reliable event delivery
		client = analytics.New(segmentAPIKey)

		fmt.Println("Segment client initialized successfully")
	} else {
		fmt.Println("Telemetry is disabled, not initializing client")
	}

	instance = &Telemetry{
		client:      client,
		serviceName: getServiceName(),
		enabled:     isTelemetryEnabled(),
		platform:    getPlatformInfo(),
	}

	// Send an initialization event to verify everything is working
	if instance.enabled && instance.client != nil {
		_ = instance.SendEvent("TelemetryInitialized", map[string]interface{}{
			"timestamp": time.Now().String(),
		})
	}
}

func GetInstance() *Telemetry {
	once.Do(createTelemetry)
	return instance
}

func (t *Telemetry) Flush() {
	if t.client != nil {
		fmt.Println("Flushing telemetry events...")
		err := t.client.Close()
		if err != nil {
			fmt.Printf("Error flushing telemetry: %v\n", err)
		} else {
			fmt.Println("Telemetry events flushed successfully")
		}
	}
}

func (t *Telemetry) SendEvent(eventName string, properties map[string]interface{}) error {
	if !t.enabled {
		fmt.Println("Telemetry disabled, not sending event:", eventName)
		return nil
	}

	if t.client == nil {
		fmt.Println("Telemetry client is nil, not sending event:", eventName)
		return fmt.Errorf("telemetry client is nil")
	}

	// Add common properties
	if properties == nil {
		properties = make(map[string]interface{})
	}

	props := map[string]interface{}{
		"anonymous_id":  GetAnonymousID(),
		"os":            t.platform.OS,
		"arch":          t.platform.Arch,
		"olake_version": t.platform.OlakeVersion,
		"service":       t.serviceName,
		"environment":   getDeploymentType(),
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
	}

	for k, v := range properties {
		props[k] = v
	}

	anonymousID := GetAnonymousID()
	fmt.Printf("Sending event: %s for user: %s\n", eventName, anonymousID)

	err := t.client.Enqueue(analytics.Track{
		UserId:     anonymousID,
		Event:      eventName,
		Properties: props,
	})

	if err != nil {
		fmt.Printf("Error sending telemetry event: %v\n", err)
		return err
	}
	fmt.Printf("Event %s queued successfully\n", eventName)

	return nil
}

func isTelemetryEnabled() bool {
	return telemetryEnabled
}

func getDeploymentType() string {
	if deployment := deploymentType; deployment != "" {
		return deployment
	}
	return "development"
}

func getServiceName() string {
	if service := serviceName; service != "" {
		return service
	}
	return "olake"
}

func getPlatformInfo() platformInfo {
	return platformInfo{
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		OlakeVersion: version,
	}
}

func GetAnonymousID() string {
	idLock.Lock()
	defer idLock.Unlock()

	configDir := getConfigDir()
	idPath := filepath.Join(configDir, anonymousIDFile)

	// Read existing ID
	if idBytes, err := os.ReadFile(idPath); err == nil {
		return string(idBytes)
	}

	// Generate new ID
	newID := generateUUID()
	if err := os.MkdirAll(configDir, 0755); err != nil {
		fmt.Printf("Error creating config dir: %v\n", err)
	}
	if err := os.WriteFile(idPath, []byte(newID), 0600); err != nil {
		fmt.Printf("Error writing anonymous ID: %v\n", err)
	}
	return newID
}

func getConfigDir() string {
	return filepath.Join(os.TempDir(), "olake")
}

func generateUUID() string {
	hash := sha256.New()
	hash.Write([]byte(time.Now().String()))
	return hex.EncodeToString(hash.Sum(nil))[:32]
}

func ComputeConfigHash(srcPath, destPath string) string {
	if srcPath == "" || destPath == "" {
		// no config or no destination → no meaningful hash
		return ""
	}
	a, err := os.ReadFile(srcPath)
	if err != nil {
		return ""
	}
	b, err := os.ReadFile(destPath)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append(a, b...))
	return hex.EncodeToString(sum[:])
}
