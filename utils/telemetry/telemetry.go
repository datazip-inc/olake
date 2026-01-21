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
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/viper"
)

const (
	userIDFile            = "user_id"
	ipNotFoundPlaceholder = "NA"
	proxTrackURL          = "https://analytics.olake.io/mp/track"
)

type Telemetry struct {
	httpClient   *http.Client
	serviceName  string
	platform     platformInfo
	ipAddress    string
	locationInfo *LocationInfo
	userID       string
}

var telemetry *Telemetry

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
	go func() {
		// check for disable
		disabled, _ := strconv.ParseBool(os.Getenv("TELEMETRY_DISABLED"))
		if disabled {
			return
		}
		ip := getOutboundIP()
		telemetry = &Telemetry{
			httpClient: &http.Client{Timeout: 5 * time.Second},
			userID:     getUserID(),
			platform: platformInfo{
				OS:           runtime.GOOS,
				Arch:         runtime.GOARCH,
				OlakeVersion: getOlakeCLIVersion(),
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
}

func TrackDiscover(streamCount int, sourceType string) {
	go func() {
		if telemetry == nil {
			return
		}
		props := map[string]interface{}{
			"stream_count": streamCount,
			"source_type":  sourceType,
		}
		if err := telemetry.sendEvent("Discover - CLI", props); err != nil {
			logger.Debugf("Failed to send Discover event: %v", err)
		}
	}()
}

func TrackSyncStarted(syncID string, streams []*types.Stream, selectedStreams []string, fullLoadStreams, cdcStreams []types.StreamInterface, sourceType string, destinationConfig *types.WriterConfig, catalog *types.Catalog) {
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
			"full_load_streams":   len(fullLoadStreams),
			"cdc_streams":         len(cdcStreams),
			"source_type":         sourceType,
			"destination_type":    string(destinationConfig.Type),
			"catalog_type":        catalogType,
			"normalized_streams":  countNormalizedStreams(catalog),
			"partitioned_streams": countPartitionedStreams(catalog),
		}

		if err := telemetry.sendEvent("Sync Started - CLI", props); err != nil {
			logger.Debugf("Failed to send SyncStarted event: %v", err)
		}
	}()
}

func TrackSyncCompleted(syncID string, status bool, records int64) {
	go func() {
		if telemetry == nil {
			return
		}
		props := map[string]interface{}{
			"sync_id":        syncID,
			"sync_end":       time.Now(),
			"sync_status":    utils.Ternary(status, "SUCCESS", "FAILED").(string),
			"records_synced": records,
		}

		if err := telemetry.sendEvent("Sync Completed - CLI", props); err != nil {
			logger.Debugf("Failed to send SyncCompleted event: %v", err)
		}
	}()
}

func (t *Telemetry) sendEvent(eventName string, props map[string]interface{}) error {
	if t.httpClient == nil {
		return fmt.Errorf("telemetry client is nil")
	}

	// Add common properties
	if props == nil {
		props = make(map[string]interface{})
	}
	properties := map[string]interface{}{
		"os":                  t.platform.OS,
		"arch":                t.platform.Arch,
		"olake_version":       t.platform.OlakeVersion,
		"num_cpu":             t.platform.DeviceCPU,
		"service":             t.serviceName,
		"ip_address":          t.ipAddress,
		"location":            t.locationInfo,
		"distinct_id":         t.userID,
		"time":                time.Now().Unix(),
		"event_original_name": eventName,
	}

	for key, value := range properties {
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

func getUserID() string {
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

func countNormalizedStreams(catalog *types.Catalog) int {
	var count int
	_ = utils.ForEach(catalog.Streams, func(s *types.ConfiguredStream) error {
		if s.StreamMetadata.Normalization {
			count++
		}
		return nil
	})
	return count
}

func countPartitionedStreams(catalog *types.Catalog) int {
	var count int
	_ = utils.ForEach(catalog.Streams, func(s *types.ConfiguredStream) error {
		if s.StreamMetadata.PartitionRegex != "" {
			count++
		}
		return nil
	})
	return count
}

// getOlakeCLIVersion() extracts the olake version from the ENV embedded in the olake image
func getOlakeCLIVersion() string {
	version := os.Getenv("DRIVER_VERSION")
	if version == "" {
		return "Not Available"
	}
	return version
}
