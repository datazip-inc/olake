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
	analytics "github.com/segmentio/analytics-go/v3"
	"github.com/spf13/viper"
)

const (
	userIDFile            = "user_id"
	version               = "0.0.0"
	ipNotFoundPlaceholder = "NA"
	segmentAPIKey         = "AiWKKeaOKQvsOotHj5iGANpNhYG6OaM3" //nolint:gosec
)

type Telemetry struct {
	client       analytics.Client
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
		client := analytics.New(segmentAPIKey)
		telemetry = &Telemetry{
			client: client,
			userID: getUserID(),
			platform: platformInfo{
				OS:           runtime.GOOS,
				Arch:         runtime.GOARCH,
				OlakeVersion: version,
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
		defer telemetry.client.Close()
		props := map[string]interface{}{
			"stream_count": streamCount,
			"source_type":  sourceType,
		}
		if err := telemetry.sendEvent("Discover - CLI", props); err != nil {
			logger.Debugf("Failed to send Discover event: %v", err)
		}
	}()
}

func TrackSyncStarted(syncID string, streams []*types.Stream, selectedStreams []string, cdcStreams []types.StreamInterface, sourceType string, destinationConfig *types.WriterConfig, catalog *types.Catalog) {
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

		if err := telemetry.sendEvent("Sync Started - CLI", props); err != nil {
			logger.Debugf("Failed to send SyncStarted event: %v", err)
		}
	}()
}

func TrackSyncCompleted(status bool, records int64) {
	go func() {
		if telemetry == nil {
			return
		}
		defer telemetry.client.Close()
		props := map[string]interface{}{
			"sync_end":       time.Now(),
			"sync_status":    utils.Ternary(status, "SUCCESS", "FAILED").(string),
			"records_synced": records,
		}

		if err := telemetry.sendEvent("Sync Completed - CLI", props); err != nil {
			logger.Debugf("Failed to send SyncCompleted event: %v", err)
		}
	}()
}

func (t *Telemetry) sendEvent(eventName string, properties map[string]interface{}) error {
	if t.client == nil {
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
		"location":      t.locationInfo,
	}

	for k, v := range properties {
		props[k] = v
	}

	return t.client.Enqueue(analytics.Track{
		UserId:     t.userID,
		Event:      eventName,
		Properties: props,
	})
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
