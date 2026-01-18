package driver

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
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

// Full Refresh methods

func (e *Elasticsearch) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	countRes, err := e.client.Count(
		e.client.Count.WithIndex(stream.Name()),
		e.client.Count.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get document count: %s", err)
	}
	defer countRes.Body.Close()

	if countRes.IsError() {
		return nil, fmt.Errorf("elasticsearch count error: %s", countRes.String())
	}

	var countResp map[string]interface{}
	if err := json.NewDecoder(countRes.Body).Decode(&countResp); err != nil {
		return nil, fmt.Errorf("failed to parse count response: %s", err)
	}

	count := int64(0)
	if c, ok := countResp["count"].(float64); ok {
		count = int64(c)
	}

	pool.AddRecordsToSyncStats(count)
	logger.Infof("Index %s has approximately %d documents", stream.Name(), count)

	chunks := types.NewSet[types.Chunk]()
	chunks.Insert(types.Chunk{Min: nil, Max: nil})

	return chunks, nil
}

func (e *Elasticsearch) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	logger.Infof("Starting backfill for index: %s", stream.Name())

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	return e.searchAfterIteration(ctx, stream, query, OnMessage)
}

func (e *Elasticsearch) searchAfterIteration(ctx context.Context, stream types.StreamInterface, query map[string]interface{}, OnMessage abstract.BackfillMsgFn) error {
	var searchAfter []interface{}
	totalProcessed := 0

	for {
		searchBody := map[string]interface{}{
			"size": defaultSearchAfterSize,
			"sort": []interface{}{
				"_doc",
			},
		}

		for k, v := range query {
			searchBody[k] = v
		}

		if searchAfter != nil {
			searchBody["search_after"] = searchAfter
		}

		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(searchBody); err != nil {
			return fmt.Errorf("failed to encode search body: %s", err)
		}

		res, err := e.client.Search(
			e.client.Search.WithContext(ctx),
			e.client.Search.WithIndex(stream.Name()),
			e.client.Search.WithBody(&buf),
		)
		if err != nil {
			return fmt.Errorf("search request failed: %s", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			bodyBytes, _ := io.ReadAll(res.Body)
			return fmt.Errorf("elasticsearch search error: %s", string(bodyBytes))
		}

		var searchResp map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
			return fmt.Errorf("failed to parse search response: %s", err)
		}

		hits, ok := searchResp["hits"].(map[string]interface{})
		if !ok {
			break
		}

		hitsArray, ok := hits["hits"].([]interface{})
		if !ok || len(hitsArray) == 0 {
			break
		}

		for _, hit := range hitsArray {
			hitMap, ok := hit.(map[string]interface{})
			if !ok {
				continue
			}

			record := make(types.Record)

			if id, ok := hitMap["_id"].(string); ok {
				record["_id"] = id
			}

			if source, ok := hitMap["_source"].(map[string]interface{}); ok {
				for k, v := range source {
					record[k] = v
				}
			}

			if err := OnMessage(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %s", err)
			}

			totalProcessed++

			if sort, ok := hitMap["sort"].([]interface{}); ok {
				searchAfter = sort
			}
		}

		if len(hitsArray) < defaultSearchAfterSize {
			break
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	logger.Infof("Completed backfill for index %s, processed %d documents", stream.Name(), totalProcessed)
	return nil
}

// Incremental sync methods

func (e *Elasticsearch) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	primaryCursor, _ := stream.Self().Cursor()
	if primaryCursor == "" {
		return nil, nil, fmt.Errorf("cursor field not specified for incremental sync")
	}

	aggQuery := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"min_cursor": map[string]interface{}{
				"min": map[string]interface{}{
					"field": primaryCursor,
				},
			},
			"max_cursor": map[string]interface{}{
				"max": map[string]interface{}{
					"field": primaryCursor,
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(aggQuery); err != nil {
		return nil, nil, fmt.Errorf("failed to encode aggregation query: %s", err)
	}

	res, err := e.client.Search(
		e.client.Search.WithContext(ctx),
		e.client.Search.WithIndex(stream.Name()),
		e.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("aggregation query failed: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, nil, fmt.Errorf("elasticsearch aggregation error: %s", res.String())
	}

	var aggResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&aggResp); err != nil {
		return nil, nil, fmt.Errorf("failed to parse aggregation response: %s", err)
	}

	var minValue, maxValue interface{}

	if aggs, ok := aggResp["aggregations"].(map[string]interface{}); ok {
		if minAgg, ok := aggs["min_cursor"].(map[string]interface{}); ok {
			minValue = minAgg["value"]
		}
		if maxAgg, ok := aggs["max_cursor"].(map[string]interface{}); ok {
			maxValue = maxAgg["value"]
		}
	}

	logger.Infof("Cursor range for %s.%s: min=%v, max=%v", stream.Name(), primaryCursor, minValue, maxValue)
	return minValue, maxValue, nil
}

func (e *Elasticsearch) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	primaryCursor, _ := stream.Self().Cursor()
	if primaryCursor == "" {
		return fmt.Errorf("cursor field not specified for incremental sync")
	}

	var lastCursorValue interface{}
	if e.state != nil {
		lastCursorValue = e.state.GetCursor(stream.Self(), primaryCursor)
		if lastCursorValue != nil {
			// Reformat cursor value to proper type (e.g., convert numeric timestamp to time.Time)
			reformattedCursor, err := abstract.ReformatCursorValue(primaryCursor, lastCursorValue, stream)
			if err != nil {
				return fmt.Errorf("failed to reformat cursor value: %s", err)
			}
			lastCursorValue = reformattedCursor
			logger.Infof("Resuming incremental sync from cursor: %v", lastCursorValue)
		}
	}

	query := e.buildIncrementalQuery(primaryCursor, lastCursorValue)

	return e.incrementalSearchAfter(ctx, stream, primaryCursor, query, cb)
}

func (e *Elasticsearch) buildIncrementalQuery(cursorField string, lastCursor interface{}) map[string]interface{} {
	if lastCursor == nil {
		return map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
	}

	// Convert cursor value to appropriate format for Elasticsearch
	// If it's a time.Time, convert to ISO 8601 string in UTC
	cursorValue := lastCursor
	if t, ok := lastCursor.(time.Time); ok {
		cursorValue = t.UTC().Format(time.RFC3339)
	}

	return map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				cursorField: map[string]interface{}{
					"gte": cursorValue,
				},
			},
		},
	}
}

func (e *Elasticsearch) incrementalSearchAfter(ctx context.Context, stream types.StreamInterface, cursorField string, query map[string]interface{}, OnMessage abstract.BackfillMsgFn) error {
	var searchAfter []interface{}
	totalProcessed := 0
	var lastCursorValue interface{}

	for {
		searchBody := map[string]interface{}{
			"size": defaultSearchAfterSize,
			"sort": []interface{}{
				map[string]interface{}{cursorField: "asc"},
			},
		}

		for k, v := range query {
			searchBody[k] = v
		}

		if searchAfter != nil {
			searchBody["search_after"] = searchAfter
		}

		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(searchBody); err != nil {
			return fmt.Errorf("failed to encode search body: %s", err)
		}

		res, err := e.client.Search(
			e.client.Search.WithContext(ctx),
			e.client.Search.WithIndex(stream.Name()),
			e.client.Search.WithBody(&buf),
		)
		if err != nil {
			return fmt.Errorf("incremental search failed: %s", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			// Read the error response body
			bodyBytes, _ := io.ReadAll(res.Body)
			return fmt.Errorf("elasticsearch search error [%s]: %s", res.Status(), string(bodyBytes))
		}

		var searchResp map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
			return fmt.Errorf("failed to parse search response: %s", err)
		}

		hits, ok := searchResp["hits"].(map[string]interface{})
		if !ok {
			break
		}

		hitsArray, ok := hits["hits"].([]interface{})
		if !ok || len(hitsArray) == 0 {
			break
		}

		for _, hit := range hitsArray {
			hitMap, ok := hit.(map[string]interface{})
			if !ok {
				continue
			}

			record := make(types.Record)

			if id, ok := hitMap["_id"].(string); ok {
				record["_id"] = id
			}

			if source, ok := hitMap["_source"].(map[string]interface{}); ok {
				for k, v := range source {
					record[k] = v
				}

				if cursorVal, ok := source[cursorField]; ok {
					lastCursorValue = cursorVal
				}
			}

			if err := OnMessage(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %s", err)
			}

			totalProcessed++

			if sort, ok := hitMap["sort"].([]interface{}); ok {
				searchAfter = sort
			}
		}

		if lastCursorValue != nil && e.state != nil {
			e.state.SetCursor(stream.Self(), cursorField, lastCursorValue)
		}

		if len(hitsArray) < defaultSearchAfterSize {
			break
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	logger.Infof("Completed incremental sync for index %s, processed %d documents", stream.Name(), totalProcessed)
	return nil
}

// CDC methods - not supported
func (e *Elasticsearch) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}

func (e *Elasticsearch) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}

func (e *Elasticsearch) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
	return fmt.Errorf("CDC is not supported for Elasticsearch")
}
