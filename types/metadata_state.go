package types

type MetadataState struct {
	ID                      any      `json:"id,omitempty"`
	State                   any      `json:"state,omitempty"`
	FullRefreshCommittedIDs []string `json:"full_refresh_committed_ids,omitempty"`
}
