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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/version"
	"github.com/spf13/viper"
)

const (
	userIDFile            = "user_id"
	ipNotFoundPlaceholder = "NA"
	proxTrackURL          = "https://analytics.olake.io/mp/track"
	// flushTimeout bounds how long a failing command waits for its report to leave the process.
	flushTimeout   = 5 * time.Second
	eventPropsFile = "telemetry.json"
	distinctIDKey  = "distinct_id"
	serviceKey     = "service"
	defaultService = "CLI"
	// maxUIConfigSize bounds the read: the file is written by another process.
	maxEventPropsFileSize = 1 << 20 // 1 MiB
)

// Event names, qualified with the event source at send time, e.g. "Sync Started - CLI".
const (
	eventDiscover      = "Discover"
	eventSyncStarted   = "Sync Started"
	eventSyncCompleted = "Sync Completed"
	eventFailure       = "Failure"
)

type Telemetry struct {
	httpClient   *http.Client
	service      string
	platform     platformInfo
	ipAddress    string
	locationInfo *LocationInfo
	userID       string
	// eventProps is the telemetry.json object merged onto every event last, so caller keys override the CLI defaults.
	eventProps map[string]interface{}
}

var telemetry *Telemetry

var (
	disabledOnce sync.Once
	disabled     bool

	// initOnce keeps Init from starting a second setup goroutine. PersistentPreRunE
	// runs twice per process (protocol init Execute, then RegisterDriver Execute);
	// a second close(initDone) panics and kills the CLI with exit 2.
	initOnce sync.Once

	// initDone closes once Init's background setup finishes. Init does network calls, so an
	// event sent right after start would otherwise find telemetry nil and be dropped silently.
	initDone = make(chan struct{})

	// inflight tracks handed-off events so a command about to exit can wait for them.
	inflight sync.WaitGroup
)

// Disabled reports whether telemetry is turned off via the TELEMETRY_DISABLED env var.
func Disabled() bool {
	disabledOnce.Do(func() {
		disabled, _ = strconv.ParseBool(os.Getenv("TELEMETRY_DISABLED"))
	})
	return disabled
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

func Init() {
	initOnce.Do(func() {
		go func() {
			defer close(initDone)
			// check for disable
			if Disabled() {
				return
			}
			ip := getOutboundIP()
			eventProps := loadEventProps()
			telemetry = &Telemetry{
				httpClient: &http.Client{Timeout: 5 * time.Second},
				userID:     getUserID(eventProps),
				service:    getService(eventProps),
				eventProps: eventProps,
				platform: platformInfo{
					OS:           runtime.GOOS,
					Arch:         runtime.GOARCH,
					OlakeVersion: version.GetOlakeCLIVersion(),
					DeviceCPU:    fmt.Sprintf("%d cores", runtime.NumCPU()),
				},
				ipAddress: ip,
			}

			if ip != ipNotFoundPlaceholder {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				loc, err := getLocationFromIP(ctx, ip)
				if err == nil {
					telemetry.locationInfo = &loc
				} else {
					logger.Debugf("Failed to fetch location for IP %s: %v", ip, err)
					telemetry.locationInfo = &LocationInfo{
						Country: "NA",
						Region:  "NA",
						City:    "NA",
					}
				}
			}
		}()
	})
}

// send runs an event in the background, recording it so Flush can wait for it. Panics are
// contained: one here would otherwise kill the command being reported on.
func send(name string, build func()) {
	inflight.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Debugf("recovered from panic while sending %s event: %v", name, r)
			}
			inflight.Done()
		}()
		<-initDone
		if telemetry == nil {
			return
		}
		build()
	}()
}

// Flush waits for handed-off events. Commands call it before exiting:
func Flush() {
	if Disabled() {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		inflight.Wait()
	}()
	select {
	case <-done:
	case <-time.After(flushTimeout):
		logger.Debugf("telemetry flush timed out after %s", flushTimeout)
	}
}

func TrackDiscover(streamCount int, sourceType string) {
	send("discover", func() {
		props := map[string]interface{}{
			"stream_count": streamCount,
			"source_type":  sourceType,
		}
		if err := telemetry.sendEvent(eventDiscover, props); err != nil {
			logger.Debugf("Failed to send Discover event: %v", err)
		}
	})
}

// addStreamMix copies the per-sync stream breakdown onto an event. It rides on both sync
// events, which are lost independently, and seven integers are cheap.
func addStreamMix(props map[string]interface{}, mix types.StreamMix) {
	props["full_refresh_streams_count"] = mix.FullRefresh
	props["incremental_streams_count"] = mix.Incremental
	props["cdc_streams_count"] = mix.CDC
	props["strict_cdc_streams_count"] = mix.StrictCDC
	props["selected_streams_count"] = mix.Selected
	props["normalized_streams_count"] = mix.Normalized
	props["partitioned_streams_count"] = mix.Partitioned
}

// destinationShape reads the destination type and catalog type from the destination config
func destinationShape(destinationConfig *types.WriterConfig) (destinationType, catalogType string) {
	if destinationConfig == nil {
		return "", ""
	}
	destinationType = string(destinationConfig.Type)
	if destinationConfig.Type != types.Iceberg {
		return destinationType, ""
	}
	if writerConfig, ok := destinationConfig.WriterConfig.(map[string]interface{}); ok {
		catalogType, _ = writerConfig["catalog_type"].(string)
	}
	return destinationType, catalogType
}

func TrackSyncStarted(syncID string, mix types.StreamMix, sourceType string, destinationConfig *types.WriterConfig, configuredStreams int) {
	destinationType, catalogType := destinationShape(destinationConfig)

	send("sync started", func() {
		props := map[string]interface{}{
			"sync_start":                  time.Now(),
			"sync_id":                     syncID,
			"stream_count":                configuredStreams,
			"source_type":                 sourceType,
			"destination_type":            destinationType,
			"catalog_type":                catalogType,
			"stream_with_pos_update_mode": mix.StreamWithPosUpdateType,
		}

		addStreamMix(props, mix)

		if err := telemetry.sendEvent(eventSyncStarted, props); err != nil {
			logger.Debugf("Failed to send SyncStarted event: %v", err)
		}
	})
}

func TrackSyncCompleted(syncID string, mix types.StreamMix, destinationConfig *types.WriterConfig, status bool, records, bytesRead int64) {
	destinationType, catalogType := destinationShape(destinationConfig)

	send("sync completed", func() {
		props := map[string]interface{}{
			"sync_id":          syncID,
			"sync_end":         time.Now(),
			"sync_status":      utils.Ternary(status, "SUCCESS", "FAILED").(string),
			"records_synced":   records,
			"bytes_read":       bytesRead,
			"destination_type": destinationType,
			"catalog_type":     catalogType,
		}
		addStreamMix(props, mix)

		if err := telemetry.sendEvent(eventSyncCompleted, props); err != nil {
			logger.Debugf("Failed to send SyncCompleted event: %v", err)
		}
	})
}

// TrackFailure reports why a command failed. The payload is a classification, not a
// description: every field is a constant from this repo or a code the vendor defines, so no
// config value, server message or user input can reach it. Absent fields are normal.
func TrackFailure(command, errorSource, syncID string, f errs.Failure) {
	send("failure - cli", func() {
		// The rest of the run's shape is on the sync events, reachable through sync_id.
		props := map[string]interface{}{
			"command":       command,
			"error_source":  errorSource,
			"category":      string(f.Category),
			"classified_by": f.ClassifiedBy,
		}
		// Absent for every command except sync.
		if syncID != "" {
			props["sync_id"] = syncID
		}
		// Only send what exists: an absent code is a legitimate slice, not a value to fake.
		if f.Code != "" {
			props["code"] = f.Code
		}
		if f.ErrorType != "" {
			props["error_type"] = f.ErrorType
		}

		if err := telemetry.sendEvent(eventFailure, props); err != nil {
			logger.Debugf("Failed to send Failure event: %v", err)
		}
	})
}

func (t *Telemetry) sendEvent(event string, props map[string]interface{}) error {
	if t.httpClient == nil {
		return fmt.Errorf("telemetry client is nil")
	}

	eventName := fmt.Sprintf("%s - %s", event, t.service)

	// Add common properties
	if props == nil {
		props = make(map[string]interface{})
	}
	properties := map[string]interface{}{
		"os":                  t.platform.OS,
		"arch":                t.platform.Arch,
		"olake_version":       t.platform.OlakeVersion,
		"num_cpu":             t.platform.DeviceCPU,
		"service":             t.service,
		"ip_address":          t.ipAddress,
		"location":            t.locationInfo,
		"distinct_id":         t.userID,
		"time":                time.Now().Unix(),
		"event_original_name": eventName,
	}

	for key, value := range properties {
		props[key] = value
	}

	// Applied last so the event props can override the default props.
	for key, value := range t.eventProps {
		props[key] = value
	}

	body := map[string]interface{}{
		"event":      eventName,
		"properties": props,
	}
	propsBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", proxTrackURL, strings.NewReader(string(propsBody)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send telemetry event, status: %s, response: %s", resp.Status, string(respBody))
	}
	return nil
}

func getOutboundIP() string {
	ip := []byte(ipNotFoundPlaceholder)
	// Timeout to the client so that init dosen't hang forever
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("https://api.ipify.org?format=text")

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

// getService returns telemetry.json's service, defaulting to the CLI.
func getService(eventProps map[string]interface{}) string {
	if service, ok := eventProps[serviceKey].(string); ok {
		if service = strings.TrimSpace(service); service != "" {
			return service
		}
	}
	return defaultService
}

// loadEventProps reads telemetry.json; an absent or malformed one is dropped, never failing the run.
func loadEventProps() map[string]interface{} {
	configFolder := viper.GetString(constants.ConfigFolder)
	if configFolder == "" {
		return nil
	}

	file, err := os.Open(filepath.Join(configFolder, eventPropsFile))
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Debugf("Failed to open %s: %v", eventPropsFile, err)
		}
		return nil
	}
	defer file.Close()

	var props map[string]interface{}
	if err := json.NewDecoder(io.LimitReader(file, maxEventPropsFileSize)).Decode(&props); err != nil {
		logger.Debugf("Ignoring %s, not a JSON object: %v", eventPropsFile, err)
		return nil
	}
	return props
}

// getUserID prefers telemetry.json's distinct_id; older callers still write user_id.txt.
func getUserID(eventProps map[string]interface{}) string {
	if id, ok := eventProps[distinctIDKey].(string); ok {
		if id = strings.TrimSpace(id); id != "" {
			return id
		}
	}

	// check if id file exists
	configFolder := viper.GetString(constants.ConfigFolder)
	if configFolder != "" {
		if idBytes, err := os.ReadFile(filepath.Join(configFolder, fmt.Sprintf("%s.txt", userIDFile))); err == nil {
			uID := strings.Trim(string(idBytes), `"`)
			return uID
		}
	}

	// Generate new ID
	hash := sha256.New()
	hash.Write([]byte(time.Now().String()))
	generatedID := hex.EncodeToString(hash.Sum(nil))[:32]
	_ = logger.FileLogger(generatedID, userIDFile, ".txt")
	return generatedID
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
