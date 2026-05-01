package types

import (
	"fmt"

	"github.com/goccy/go-json"
)

type MetadataState struct {
	ID                      any      `json:"id,omitempty"`
	State                   any      `json:"state,omitempty"`
	FullRefreshCommittedIDs []string `json:"full_refresh_committed_ids,omitempty"`
	// nil/true = overlap window open -> inserts emit "i" (equality delete + write).
	// false    = steady state -> inserts emit "c" (write only).
	DedupInserts *bool `json:"dedup_inserts,omitempty"`
}

// SetMetadataState returns a MetadataState with State stored as a JSON string.
// If mtState is already a *MetadataState, its non-State fields (DedupInserts,
// FullRefreshCommittedIDs, etc.) are preserved on a *copy* of the input — the
// caller's struct is never mutated. Otherwise mtState itself becomes State.
// A nil rawState produces a result with State unset so it's omitted from JSON,
// leaving any previously-persisted `state` property untouched downstream.
func SetMetadataState(mtState any, threadID string) (*MetadataState, error) {
	result, rawState := &MetadataState{ID: threadID}, mtState
	if existing, ok := mtState.(*MetadataState); ok {
		clone := *existing
		result, rawState = &clone, existing.State
		if threadID != "" {
			result.ID = threadID
		}
	}

	switch s := rawState.(type) {
	case nil:
		// leave result.State unset; omitempty drops it from the marshaled JSON
	case string:
		result.State = s
	default:
		stateBytes, err := json.Marshal(s)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal metadata state: %s", err)
		}
		result.State = string(stateBytes)
	}

	return result, nil
}
