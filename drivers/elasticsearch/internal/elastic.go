package driver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/elastic/go-elasticsearch/v8"
)

const (
	defaultSearchAfterSize = 10000
	defaultBatchSize       = 1000
)

// Type mappings from Elasticsearch to OLake
var esTypeToDataTypes = map[string]types.DataType{
	"text":         types.String,
	"keyword":      types.String,
	"long":         types.Int64,
	"integer":      types.Int64,
	"short":        types.Int64,
	"byte":         types.Int64,
	"double":       types.Float64,
	"float":        types.Float64,
	"half_float":   types.Float64,
	"scaled_float": types.Float64,
	"date":         types.Timestamp,
	"boolean":      types.Bool,
	"binary":       types.String,
	"object":       types.String,
	"nested":       types.String,
	"geo_point":    types.String,
	"geo_shape":    types.String,
	"ip":           types.String,
}

// Elasticsearch driver implementation
type Elasticsearch struct {
	client     *elasticsearch.Client
	config     *Config
	state      *types.State
	CDCSupport bool
}

// Driver interface methods

func (e *Elasticsearch) CDCSupported() bool {
	return e.CDCSupport
}

func (e *Elasticsearch) Setup(ctx context.Context) error {
	err := e.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	cfg := elasticsearch.Config{}

	if e.config.CloudID != "" {
		cfg.CloudID = e.config.CloudID
	} else {
		scheme := "http"
		if e.config.UseSSL {
			scheme = "https"
		}
		cfg.Addresses = []string{
			fmt.Sprintf("%s://%s:%d", scheme, e.config.Host, e.config.Port),
		}
	}

	if e.config.APIKey != "" {
		cfg.APIKey = e.config.APIKey
	} else if e.config.Username != "" {
		cfg.Username = e.config.Username
		cfg.Password = e.config.Password
	}

	if e.config.UseSSL {
		cfg.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch client: %s", err)
	}

	res, err := client.Info()
	if err != nil {
		return fmt.Errorf("failed to connect to elasticsearch: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch returned error: %s", res.String())
	}

	e.client = client
	e.config.RetryCount = utils.Ternary(e.config.RetryCount <= 0, 1, e.config.RetryCount+1).(int)

	logger.Info("Successfully connected to Elasticsearch")
	return nil
}

func (e *Elasticsearch) StateType() types.StateType {
	return types.GlobalType
}

func (e *Elasticsearch) SetupState(state *types.State) {
	e.state = state
}

func (e *Elasticsearch) GetConfigRef() abstract.Config {
	e.config = &Config{}
	return e.config
}

func (e *Elasticsearch) Spec() any {
	return Config{}
}

func (e *Elasticsearch) Close() {
	logger.Info("Closing Elasticsearch connection")
}

func (e *Elasticsearch) Type() string {
	return string(constants.Elasticsearch)
}

func (e *Elasticsearch) MaxConnections() int {
	return e.config.MaxThreads
}

func (e *Elasticsearch) MaxRetries() int {
	return e.config.RetryCount
}

// Discovery methods

func (e *Elasticsearch) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Elasticsearch index pattern: %s", e.config.Index)

	res, err := e.client.Cat.Indices(
		e.client.Cat.Indices.WithIndex(e.config.Index),
		e.client.Cat.Indices.WithFormat("json"),
		e.client.Cat.Indices.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve indices: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch returned error: %s", res.String())
	}

	var indices []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		return nil, fmt.Errorf("failed to parse indices response: %s", err)
	}

	indexNames := []string{}
	for _, idx := range indices {
		if name, ok := idx["index"].(string); ok {
			if !strings.HasPrefix(name, ".") {
				indexNames = append(indexNames, name)
			}
		}
	}

	return indexNames, nil
}

func (e *Elasticsearch) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	stream := types.NewStream(streamName, "elasticsearch", nil)

	res, err := e.client.Indices.GetMapping(
		e.client.Indices.GetMapping.WithIndex(streamName),
		e.client.Indices.GetMapping.WithContext(ctx),
	)
	if err != nil {
		return stream, fmt.Errorf("failed to get mapping for index %s: %s", streamName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return stream, fmt.Errorf("elasticsearch returned error: %s", res.String())
	}

	var mappings map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&mappings); err != nil {
		return stream, fmt.Errorf("failed to parse mapping response: %s", err)
	}

	if indexMapping, ok := mappings[streamName].(map[string]interface{}); ok {
		if mappingsObj, ok := indexMapping["mappings"].(map[string]interface{}); ok {
			if properties, ok := mappingsObj["properties"].(map[string]interface{}); ok {
				e.processProperties(stream, properties, "")
			}
		}
	}

	stream.WithPrimaryKey("_id")
	stream.UpsertField("_id", types.String, false)

	// Declare supported sync modes
	stream.SupportedSyncModes.Insert(types.FULLREFRESH)
	stream.SupportedSyncModes.Insert(types.INCREMENTAL)

	return stream, nil
}

func (e *Elasticsearch) processProperties(stream *types.Stream, properties map[string]interface{}, prefix string) {
	for fieldName, fieldDef := range properties {
		fullFieldName := fieldName
		if prefix != "" {
			fullFieldName = prefix + "." + fieldName
		}

		if fieldDefMap, ok := fieldDef.(map[string]interface{}); ok {
			fieldType := "text"
			if t, ok := fieldDefMap["type"].(string); ok {
				fieldType = t
			}

			datatype := types.String
			if val, found := esTypeToDataTypes[fieldType]; found {
				datatype = val
			}

			stream.UpsertField(fullFieldName, datatype, true)
			stream.WithCursorField(fullFieldName)

			if fieldType == "object" || fieldType == "nested" {
				if nestedProps, ok := fieldDefMap["properties"].(map[string]interface{}); ok {
					e.processProperties(stream, nestedProps, fullFieldName)
				}
			}
		}
	}
}
