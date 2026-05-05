package types

import (
	"fmt"

	"github.com/goccy/go-json"
)

type MetadataState struct {
	ID                      any      `json:"id,omitempty"`
	State                   any      `json:"state,omitempty"`
	FullRefreshCommittedIDs []string `json:"full_refresh_committed_ids,omitempty"`
	// nil/true = overlap window open -> inserts emit "i" (equality delete + write), prevents phantom reads
	// false    = steady state -> inserts emit "c" (write only).
	DedupInserts *bool `json:"dedup_inserts,omitempty"`
}

// SetDedupInserts sets DedupInserts on ms only when both ms and dedupInserts are non-nil.
func SetDedupInserts(ms *MetadataState, dedupInserts *bool) {
	if ms != nil && dedupInserts != nil {
		ms.DedupInserts = dedupInserts
	}
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
