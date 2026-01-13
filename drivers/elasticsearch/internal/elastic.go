package driver

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "strings"
    "time"

    "github.com/elastic/go-elasticsearch/v8"

    "github.com/datazip-inc/olake/constants"
    "github.com/datazip-inc/olake/destination"
    "github.com/datazip-inc/olake/drivers/abstract"
    "github.com/datazip-inc/olake/types"
    "github.com/datazip-inc/olake/utils/logger"
)

type Elasticsearch struct {
    config *Config
    client *elasticsearch.Client
    state  *types.State
}

func (e *Elasticsearch) GetConfigRef() abstract.Config {
    e.config = &Config{}
    return e.config
}

func (e *Elasticsearch) Spec() any { return Config{} }

func (e *Elasticsearch) Type() string { return "elasticsearch" }

func (e *Elasticsearch) MaxConnections() int { return 1 }
func (e *Elasticsearch) MaxRetries() int { return constants.DefaultRetryCount }

func (e *Elasticsearch) CDCSupported() bool { return false }

func (e *Elasticsearch) SetupState(state *types.State) { e.state = state }

func (e *Elasticsearch) Setup(ctx context.Context) error {
    if err := e.config.Validate(); err != nil {
        return fmt.Errorf("config validation failed: %s", err)
    }
    cfg := elasticsearch.Config{Addresses: []string{e.config.Host}}
    client, err := elasticsearch.NewClient(cfg)
    if err != nil {
        return fmt.Errorf("failed to create elasticsearch client: %s", err)
    }
    e.client = client
    // light ping by performing info
    _, err = e.client.Info()
    if err != nil {
        return fmt.Errorf("failed to ping elasticsearch: %s", err)
    }
    return nil
}

func (e *Elasticsearch) Close() error {
    e.client = nil
    return nil
}

func (e *Elasticsearch) GetStreamNames(ctx context.Context) ([]string, error) {
    if e.config == nil {
        return nil, fmt.Errorf("config not initialized")
    }
    return []string{e.config.Index}, nil
}

func (e *Elasticsearch) ProduceSchema(ctx context.Context, stream string) (*types.Stream, error) {
    logger.Infof("producing schema for index [%s] (experimental - raw JSON)", stream)
    s := types.NewStream(stream, "elasticsearch", nil).WithSyncMode(types.FULLREFRESH)
    return s, nil
}

// GetOrSplitChunks returns a single chunk representing the whole index
func (e *Elasticsearch) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
    // Single chunk for entire index. We intentionally avoid complex counting
    // to keep this PoC minimal and defensive.
    chunks := types.NewSet[types.Chunk](types.Chunk{Min: nil, Max: nil})
    return chunks, nil
}

func (e *Elasticsearch) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
    if e.client == nil {
        return fmt.Errorf("client not initialized")
    }

    // Prepare search body for match_all
    body := `{"query":{"match_all":{}}}`
    // initial search with scroll
    res, err := e.client.Search(
        e.client.Search.WithContext(ctx),
        e.client.Search.WithIndex(stream.Name()),
        e.client.Search.WithScroll(time.Minute),
        e.client.Search.WithSize(1000),
        e.client.Search.WithBody(strings.NewReader(body)),
    )
    if err != nil {
        return fmt.Errorf("search failed: %s", err)
    }
    var scrollID string
    for {
        var resp map[string]any
        dec := json.NewDecoder(res.Body)
        if err := dec.Decode(&resp); err != nil {
            _ = res.Body.Close()
            if err == io.EOF {
                break
            }
            return fmt.Errorf("invalid search response: %s", err)
        }
        // close the body immediately after decoding to avoid defers in loop
        _ = res.Body.Close()

        // extract scroll id
        if sid, ok := resp["_scroll_id"].(string); ok {
            scrollID = sid
        }

        // navigate hits
        hitsObj, ok := resp["hits"].(map[string]any)
        if !ok {
            break
        }
        hitsArr, ok := hitsObj["hits"].([]any)
        if !ok || len(hitsArr) == 0 {
            break
        }

        for _, h := range hitsArr {
            if hit, ok := h.(map[string]any); ok {
                // prefer _source field; if missing, attempt to use whole hit
                var doc map[string]any
                if src, ok := hit["_source"].(map[string]any); ok {
                    doc = src
                } else {
                    // defensive fallback: use hit as-is
                    doc = hit
                }
                if err := processFn(ctx, doc); err != nil {
                    // attempt to clear scroll before returning
                    if scrollID != "" {
                        _ = e.clearScroll(context.Background(), scrollID)
                    }
                    return fmt.Errorf("failed to process document: %s", err)
                }
            }
        }

        // get next batch via scroll
        if scrollID == "" {
            break
        }
        nextRes, err := e.client.Scroll(e.client.Scroll.WithContext(ctx), e.client.Scroll.WithScrollID(scrollID), e.client.Scroll.WithScroll(time.Minute))
        if err != nil {
            _ = e.clearScroll(context.Background(), scrollID)
            return fmt.Errorf("scroll request failed: %s", err)
        }
        if nextRes == nil {
            break
        }
        // assign new response and continue
        res = nextRes
    }

    // clear scroll context
    if scrollID != "" {
        _ = e.clearScroll(context.Background(), scrollID)
    }
    return nil
}

func (e *Elasticsearch) clearScroll(ctx context.Context, scrollID string) error {
    if e.client == nil || scrollID == "" {
        return nil
    }
    res, err := e.client.ClearScroll(e.client.ClearScroll.WithScrollID(scrollID), e.client.ClearScroll.WithContext(ctx))
    if err != nil {
        return err
    }
    defer res.Body.Close()
    return nil
}

// Unsupported incremental/CDC methods
func (e *Elasticsearch) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
    return nil, nil, fmt.Errorf("incremental not supported")
}

func (e *Elasticsearch) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
    return fmt.Errorf("incremental not supported")
}

func (e *Elasticsearch) PreCDC(ctx context.Context, streams []types.StreamInterface) error { return fmt.Errorf("cdc not supported") }
func (e *Elasticsearch) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
    return fmt.Errorf("cdc not supported")
}
func (e *Elasticsearch) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
    return fmt.Errorf("cdc not supported")
}
