package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

func (e *Elasticsearch) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	countRes, err := e.client.Count(
		e.client.Count.WithIndex(stream.Name()),
		e.client.Count.WithContext(ctx),
	)
if err != nil {
		return nil, fmt.Errorf("failed to get document count: %s", err)
	}
	defer func() {
		if err := countRes.Body.Close(); err != nil {
			logger.Warnf("Failed to close response body: %v", err)
		}
	}()

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
		defer func() {
			if err := res.Body.Close(); err != nil {
				logger.Warnf("Failed to close response body: %v", err)
			}
		}()

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
