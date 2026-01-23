package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

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
