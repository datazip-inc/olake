package bigquery

import (
	"context"
	"fmt"
	"iter"

	"cloud.google.com/go/bigquery"
	"github.com/datazip-inc/olake/drivers"
	"github.com/datazip-inc/olake/types"
	"google.golang.org/api/iterator"
)

type Config struct {
	ProjectID string `json:"project_id"`
	DatasetID string `json:"dataset_id"`
	TableID   string `json:"table_id"`
}

type BigQuerySource struct {
	projectID string
	datasetID string
	tableID   string
	client    *bigquery.Client
}

func New(cfg Config) (drivers.Source, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery client: %w", err)
	}
	return &BigQuerySource{
		projectID: cfg.ProjectID,
		datasetID: cfg.DatasetID,
		tableID:   cfg.TableID,
		client:    client,
	}, nil
}

func (s *BigQuerySource) Stream(ctx context.Context, opts ...drivers.StreamOption) (iter.Seq2[types.Record, error], error) {
    // Default: full table scan
    query := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", s.projectID, s.datasetID, s.tableID)

    // TODO: Add incremental logic when opts contain last bookmark
    // Example placeholder for future CDC support
    // for _, opt := range opts {
    //     if bookmark, ok := opt.(drivers.BookmarkOption); ok && bookmark.Value != nil {
    //         query = fmt.Sprintf("%s WHERE updated_at > @last_ts", query)
    //         // add parameter binding
    //     }
    // }

    q := s.client.Query(query)
    it, err := q.Read(ctx)
    if err != nil {
        return nil, err
    }

    return func(yield func(types.Record, error) bool) {
        for {
            var values []bigquery.Value
            err := it.Next(&values)
            if err == iterator.Done {
                break
            }
            if err != nil {
                if !yield(nil, err) {
                    return
                }
                continue
            }
            rec := make(types.Record)
            for i, v := range values {
                rec[fmt.Sprintf("col_%d", i)] = v
            }
            if !yield(rec, nil) {
                return
            }
        }
    }, nil
}
func (s *BigQuerySource) Close() error {
	return s.client.Close()
}