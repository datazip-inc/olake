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
	"runtime"
	"sync"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	analytics "github.com/segmentio/analytics-go/v3"
)

type Telemetry struct {
	client        analytics.Client
	serviceName   string
	platform      platformInfo
	ipAddress     string
	locationInfo  *LocationInfo
	locationMutex sync.Mutex
	locationChan  chan struct{}
}

var telemetry *Telemetry

const (
	anonymousIDFile       = "telemetry_id"
	version               = "0.0.0"
	ipNotFoundPlaceholder = "NA"
	segmentAPIKey         = "RFynIrEFUsaTRgKumlSqfme6DRPymYfN"
)

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

func init() {
	ip := getOutboundIP()
	client := analytics.New(segmentAPIKey)

	telemetry = &Telemetry{
		client:       client,
		platform:     getPlatformInfo(),
		ipAddress:    ip,
		locationChan: make(chan struct{}),
	}

	if ip != ipNotFoundPlaceholder {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			location, err := getLocationFromIP(ctx, ip)
			if err == nil {
				telemetry.locationMutex.Lock()
				telemetry.locationInfo = &location
				telemetry.locationMutex.Unlock()
			}
			close(telemetry.locationChan)
		}()
	} else {
		close(telemetry.locationChan)
	}
}

func TrackDiscover(streamCount int, sourceType string) {
	go func() {
		if telemetry == nil {
			return
		}
		defer func() {
			logger.Infof("Discover completed, clean up going on!")
			telemetry.client.Close()
		}()
		props := map[string]interface{}{
			"stream_count": streamCount,
			"source_type":  sourceType,
		}
		if err := telemetry.sendEvent("Discover-Event", props); err != nil {
			logger.Errorf("Failed to send Discover event: %v", err)
		}
	}()
}

func TrackSyncStarted(streams []*types.Stream, selectedStreams []string, cdcStreams []types.StreamInterface, syncID, sourceType string, destinationConfig *types.WriterConfig, catalog *types.Catalog) {
	go func() {
		if telemetry == nil {
			return
		}
		catalogType := ""
		if string(destinationConfig.Type) == "ICEBERG" {
			catalogType = destinationConfig.WriterConfig.(map[string]interface{})["catalog_type"].(string)
		}
		props := map[string]interface{}{
			"sync_start":          time.Now(),
			"sync_id":             syncID,
			"stream_count":        len(streams),
			"selected_count":      len(selectedStreams),
			"cdc_streams":         len(cdcStreams),
			"source_type":         sourceType,
			"destination_type":    string(destinationConfig.Type),
			"catalog_type":        catalogType,
			"normalized_streams":  countNormalizedStreams(catalog),
			"partitioned_streams": countPartitionedStreams(catalog),
		}

		if err := telemetry.sendEvent("SyncStart-Event", props); err != nil {
			logger.Errorf("Failed to send SyncStarted event: %v", err)
		}
	}()
}

func TrackSyncCompleted(err error, records int64) {
	go func() {
		if telemetry == nil {
			return
		}
		defer func() {
			logger.Infof("Sync completed, clean up going on!")
			telemetry.client.Close()
		}()
		props := map[string]interface{}{
			"sync_end":       time.Now(),
			"sync_status":    utils.Ternary(err == nil, "SUCCESS", "FAILED").(string),
			"records_synced": records,
		}

		if err := telemetry.sendEvent("SyncCompleted", props); err != nil {
			logger.Errorf("Failed to send SyncCompleted event: %v", err)
		}
	}()
}

func (t *Telemetry) sendEvent(eventName string, properties map[string]interface{}) error {
	if t.client == nil {
		logger.Warn("Telemetry client is nil, not sending event:", eventName)
		return fmt.Errorf("telemetry client is nil")
	}

	// Add common properties
	if properties == nil {
		properties = make(map[string]interface{})
	}

	props := map[string]interface{}{
		"os":            t.platform.OS,
		"arch":          t.platform.Arch,
		"olake_version": t.platform.OlakeVersion,
		"num_cpu":       t.platform.DeviceCPU,
		"service":       t.serviceName,
		"ip_address":    t.ipAddress,
		"location":      t.getLocationWithTimeout(),
	}

	for k, v := range properties {
		props[k] = v
	}

	return t.client.Enqueue(analytics.Track{
		UserId:     fmt.Sprintf("olake-cli-%s", time.Now()),
		Event:      eventName,
		Properties: props,
	})
}

func getPlatformInfo() platformInfo {
	return platformInfo{
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		OlakeVersion: version,
		DeviceCPU:    fmt.Sprintf("%d cores", runtime.NumCPU()),
	}
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

func countNormalizedStreams(catalog *types.Catalog) int {
	count := 0
	for _, s := range catalog.Streams {
		if s.StreamMetadata.Normalization {
			count++
		}
	}
	return count
}

func countPartitionedStreams(catalog *types.Catalog) int {
	count := 0
	for _, s := range catalog.Streams {
		if s.StreamMetadata.PartitionRegex != "" {
			count++
		}
	}
	return count
}
