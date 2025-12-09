package internal

import (
	"context"
	"fmt"
)

func IncrementalQuery(projectID, datasetID, tableID, lastBookmark string) string {
	if lastBookmark == "" {
		return fmt.Sprintf("SELECT * FROM `%s.%s.%s`", projectID, datasetID, tableID)
	}
	return fmt.Sprintf("SELECT * FROM `%s.%s.%s` WHERE updated_at > '%s'", projectID, datasetID, tableID, lastBookmark)
}