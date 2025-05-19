package telemetry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	anonymousIDFile       = "telemetry_id"
	version               = "0.0.0" // Version 0
	ipNotFoundPlaceholder = "NA"
	syncCountsCountPrefix = "sync_counts_"
	syncMetricsFilePrefix = "sync_metrics_"
)

type Telemetry struct {
	client        analytics.Client
	serviceName   string
	enabled       bool
	platform      platformInfo
	ipAddress     string
	locationInfo  *LocationInfo
	locationMutex sync.Mutex
	locationChan  chan struct{}
}

type platformInfo struct {
	OS           string
	Arch         string
	OlakeVersion string
	DeviceCPU    string
}

type LocationInfo struct {
	Country string `json:"country"`
	Region  string `json:"region"`
	City    string `json:"city"`
}

type SyncMetrics struct {
	Total   int            `json:"total"`
	Success int            `json:"success"`
	Failed  int            `json:"failed"`
	Weeks   map[string]int `json:"weeks"` // Key format: "YYYY-Www" (e.g., "2023-W43")
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

func createTelemetry() {
	loadConfig()
	if isTelemetryEnabled() {
		fmt.Println("Telemetry is enabled")
		client = analytics.New(segmentAPIKey)
		fmt.Println("Segment client initialized successfully")
	} else {
		fmt.Println("Telemetry is disabled")
	}

	ip := getOutboundIP()
	instance = &Telemetry{
		client:       client,
		serviceName:  getServiceName(),
		enabled:      isTelemetryEnabled(),
		platform:     getPlatformInfo(),
		ipAddress:    ip,
		locationChan: make(chan struct{}), // INITIALIZE THE CHANNEL
	}

	if instance.enabled && instance.client != nil {
		_ = instance.SendEvent("TelemetryInitialized", map[string]interface{}{
			"ipAddress": ip,
			"timestamp": time.Now().String(),
		})
		if ip != ipNotFoundPlaceholder {
			go func() {
				defer func() {
					if instance.locationChan != nil {
						close(instance.locationChan)
					}
				}()
				location, err := getLocationFromIP(ip)
				if err != nil {
					return
				}
				instance.locationMutex.Lock()
				instance.locationInfo = &location
				instance.locationMutex.Unlock()
			}()
		} else {
			if instance.locationChan != nil {
				close(instance.locationChan)
			}
		}
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
		"num_cpu":       t.platform.DeviceCPU,
		"service":       t.serviceName,
		"ip_address":    t.ipAddress,
		"location":      t.getLocationWithTimeout(),
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
		DeviceCPU:    fmt.Sprintf("%d cores", runtime.NumCPU()),
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

func getOutboundIP() string {
	ip := []byte(ipNotFoundPlaceholder)
	resp, err := http.Get("https://api.ipify.org?format=text")

	if err != nil {
		return string(ip)
	}

	defer resp.Body.Close()
	ipBody, err := io.ReadAll(resp.Body)
	if err == nil {
		ip = ipBody
	}

	return string(ip)
}

func getLocationFromIP(ip string) (LocationInfo, error) {
	client := http.Client{Timeout: 1 * time.Second}
	resp, err := client.Get(fmt.Sprintf("https://ipinfo.io/%s/json", ip))
	if err != nil {
		return LocationInfo{}, err
	}
	defer resp.Body.Close()

	var info struct {
		Country string `json:"country"`
		Region  string `json:"region"`
		City    string `json:"city"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return LocationInfo{}, err
	}

	return LocationInfo{
		Country: info.Country,
		Region:  info.Region,
		City:    info.City,
	}, nil
}

func (t *Telemetry) getLocationWithTimeout() interface{} {
	// Wait up to 200ms for location lookup
	select {
	case <-t.locationChan: // Returns immediately if channel already closed
	case <-time.After(200 * time.Millisecond):
	}

	t.locationMutex.Lock()
	defer t.locationMutex.Unlock()

	if t.locationInfo != nil {
		return t.locationInfo
	}
	return "NA"
}

func (t *Telemetry) TrackSyncResult(configHash string, success bool) *SyncMetrics {
	if configHash == "" {
		return nil
	}

	// Get the anonymous ID - this function handles its own locking internally
	anonymousID := GetAnonymousID()
	metricsPath := filepath.Join(getConfigDir(), syncMetricsFilePrefix+anonymousID)

	// Read existing metrics - file operations don't need mutex protection
	metrics := make(map[string]SyncMetrics)
	if data, err := os.ReadFile(metricsPath); err == nil {
		_ = json.Unmarshal(data, &metrics) // Best-effort read
	}

	// Get current week identifier
	year, week := time.Now().ISOWeek()
	weekKey := fmt.Sprintf("%d-W%02d", year, week)

	// Initialize metrics for this config hash if missing
	if _, exists := metrics[configHash]; !exists {
		metrics[configHash] = SyncMetrics{
			Weeks: make(map[string]int),
		}
	}

	// Update metrics
	configMetrics := metrics[configHash]
	configMetrics.Total++
	if success {
		configMetrics.Success++
	} else {
		configMetrics.Failed++
	}
	configMetrics.Weeks[weekKey]++ // Increment weekly count
	metrics[configHash] = configMetrics

	// Persist updated metrics
	if data, err := json.Marshal(metrics); err == nil {
		_ = os.WriteFile(metricsPath, data, 0600) // Best-effort write
	} else {
		fmt.Printf("Failed to save sync metrics: %v\n", err)
	}

	return &configMetrics
}
