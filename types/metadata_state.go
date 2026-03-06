package types

import (
	"fmt"

	"github.com/goccy/go-json"
)

type MetadataState struct {
	ID                      any      `json:"id,omitempty"`
	State                   any      `json:"state,omitempty"`
	FullRefreshCommittedIDs []string `json:"full_refresh_committed_ids,omitempty"`
}

// SetMetadataState creates a MetadataState with State always stored as a JSON string.
// Callers must not pass a nil mtState; use the nil guard at the call site.
// If mtState is already a string it is used directly; otherwise it is JSON-marshaled into string.
func SetMetadataState(mtState any, threadID string) (*MetadataState, error) {
	metadataState := &MetadataState{ID: threadID}

	if value, ok := mtState.(string); ok {
		metadataState.State = value
	} else {
		stateBytes, err := json.Marshal(mtState)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal metadata state: %s", err)
		}
		metadataState.State = string(stateBytes)
	}

	return metadataState, nil
}
