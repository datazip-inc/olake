package telemetry

import (
	"context"
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

	"github.com/datazip-inc/olake/logger"
	analytics "github.com/segmentio/analytics-go/v3"
	"github.com/spf13/viper"
)

var (
	client           analytics.Client
	idLock           sync.Mutex
	telemetryEnabled bool
	deploymentType   string
	serviceName      string
	segmentAPIKey    string
	instance         *Telemetry
)

const (
	anonymousIDFile       = "telemetry_id"
	version               = "0.0.0"
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
	Weeks   map[string]int `json:"weeks"`
}

func loadConfig() {
	viper.SetConfigName("config-telemetry")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("../../")

	if err := viper.ReadInConfig(); err == nil {
		telemetryEnabled = viper.GetBool("telemetry.enabled")
		segmentAPIKey = viper.GetString("telemetry.segment_api_key")
		deploymentType = viper.GetString("telemetry.deployment_type")
		serviceName = viper.GetString("telemetry.service_name")
	}
}

func init() {
	loadConfig()
	ip := getOutboundIP()
	enabled := isTelemetryEnabled()

	if enabled {
		client = analytics.New(segmentAPIKey)
	}

	instance = &Telemetry{
		client:       client,
		serviceName:  serviceName,
		enabled:      enabled,
		platform:     getPlatformInfo(),
		ipAddress:    ip,
		locationChan: make(chan struct{}),
	}

	if instance.enabled {
		if ip != ipNotFoundPlaceholder {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				location, err := getLocationFromIP(ctx, ip)
				if err == nil {
					instance.locationMutex.Lock()
					instance.locationInfo = &location
					instance.locationMutex.Unlock()
				}
				close(instance.locationChan)
			}()
		} else {
			close(instance.locationChan)
		}
	}
}

func TrackDiscoverCompleted(duration float64, success bool, streamCount int, sourceType string, err error) {
	if instance == nil || !instance.enabled {
		return
	}

	props := map[string]interface{}{
		"duration_sec": duration,
		"success":      success,
		"stream_count": streamCount,
		"source_type":  sourceType,
	}
	if err != nil {
		props["error"] = err.Error()
	}

	if err := instance.sendEvent("DiscoverCompleted", props); err != nil {
		logger.Errorf("Failed to send DiscoverCompleted event: %v", err)
	}
}

func TrackSyncStarted(streamCount, selectedCount, cdcStreams int, stateFileProvided bool, configHash, sourceType, destType, catalogType string, normalized, partitioned int) {
	if instance == nil || !instance.enabled {
		return
	}

	props := map[string]interface{}{
		"stream_count":             streamCount,
		"selected_count":           selectedCount,
		"cdc_streams":              cdcStreams,
		"state_file_provided":      stateFileProvided,
		"unique_config_dstination": configHash,
		"sync_type":                getSyncType(stateFileProvided),
		"source_type":              sourceType,
		"destination_type":         destType,
		"catalog_type":             catalogType,
		"normalized_streams":       normalized,
		"partitioned_streams":      partitioned,
	}

	if err := instance.sendEvent("SyncStarted", props); err != nil {
		logger.Errorf("Failed to send SyncStarted event: %v", err)
	}
}

func TrackSyncCompleted(success bool, records, threads int64, durationSec float64, memoryMB uint64, metrics *SyncMetrics, syncError error) {
	if instance == nil || !instance.enabled {
		return
	}

	props := map[string]interface{}{
		"success":         success,
		"records_synced":  records,
		"duration_sec":    durationSec,
		"memory_usage_mb": memoryMB,
		"threads_used":    threads,
	}

	if metrics != nil {
		year, week := time.Now().ISOWeek()
		currentWeek := fmt.Sprintf("%d-W%02d", year, week)
		props["total_syncs"] = metrics.Total
		props["successful_syncs"] = metrics.Success
		props["failed_syncs"] = metrics.Failed
		props["current_week_syncs"] = metrics.Weeks[currentWeek]
		props["current_week"] = currentWeek
	}

	if syncError != nil {
		props["error"] = syncError.Error()
	}

	if err := instance.sendEvent("SyncCompleted", props); err != nil {
		logger.Errorf("Failed to send SyncCompleted event: %v", err)
	}
}

func getSyncType(stateFileProvided bool) string {
	if stateFileProvided {
		return "CDC"
	}
	return "FullRefresh"
}

func Flush() {
	if instance != nil && instance.client != nil {
		instance.client.Close()
	}
}

func (t *Telemetry) sendEvent(eventName string, properties map[string]interface{}) error {
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
		"environment":   deploymentType,
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
	}

	for k, v := range properties {
		props[k] = v
	}

	anonymousID := GetAnonymousID()
	fmt.Printf("Sending event: %s for user: %s\n", eventName, anonymousID)

	return t.client.Enqueue(analytics.Track{
		UserId:     GetAnonymousID(),
		Event:      eventName,
		Properties: props,
	})
}

func isTelemetryEnabled() bool {
	return telemetryEnabled
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

func getLocationFromIP(ctx context.Context, ip string) (LocationInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://ipinfo.io/%s/json", ip), nil)
	if err != nil {
		return LocationInfo{}, err
	}

	client := http.Client{Timeout: 1 * time.Second}
	resp, err := client.Do(req)
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

func SyncResult(configHash string, success bool) *SyncMetrics {
	if instance == nil {
		return nil
	}
	return instance.TrackSyncResult(configHash, success)
}
