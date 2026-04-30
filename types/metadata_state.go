package types

import (
	"fmt"

	"github.com/goccy/go-json"
)

type MetadataState struct {
	ID                      any      `json:"id,omitempty"`
	State                   any      `json:"state,omitempty"`
	FullRefreshCommittedIDs []string `json:"full_refresh_committed_ids,omitempty"`
	// DedupInserts signals whether CDC inserts must generate equality deletes.
	// true (or nil) = backfill committed, first CDC not yet → inserts emit "i" (equality delete).
	// false         = first CDC committed → inserts emit "c" (no equality delete, steady state).
	DedupInserts *bool `json:"dedup_inserts,omitempty"`
}

// SetMetadataState returns a MetadataState with State stored as a JSON string.
// If mtState is already a *MetadataState, its fields (e.g. DedupInserts) are preserved
// and only ID/State are normalised; otherwise mtState itself becomes State.
func SetMetadataState(mtState any, threadID string) (*MetadataState, error) {
	result, rawState := &MetadataState{ID: threadID}, mtState
	if existing, ok := mtState.(*MetadataState); ok {
		result, rawState = existing, existing.State
		if threadID != "" {
			result.ID = threadID
		}
	}

	if s, ok := rawState.(string); ok {
		result.State = s
	} else {
		stateBytes, err := json.Marshal(rawState)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal metadata state: %s", err)
		}
		result.State = string(stateBytes)
	}

	return result, nil
}
