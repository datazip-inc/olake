package internal

import (
	"context"
	"fmt"
	"iter"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func Backfill(s *bigquery.Client, projectID, datasetID, tableID string) iter.Seq2[map[string]any, error] {
	query := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", projectID, datasetID, tableID)
	q := s.Client.Query(query)
	it, err := q.Read(context.Background())
	if err != nil {
		return func(yield func(map[string]any, error) bool) { yield(nil, err) }
	}

	return func(yield func(map[string]any, error) bool) {
		for {
			var values []bigquery.Value
			err := it.Next(&values)
			if err == iterator.Done {
				break
			}
			if err != nil {
				yield(nil, err)
				continue
			}
			rec := make(map[string]any)
			for i, v := range values {
				rec[fmt.Sprintf("col_%d", i)] = v
			}
			if !yield(rec, nil) {
				return
			}
		}
	}
}