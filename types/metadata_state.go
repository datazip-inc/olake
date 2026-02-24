package types

type MetadataState struct {
	Id                      any      `json:"thread_id,omitempty"`
	State                   any      `json:"state,omitempty"`
	LatestThreadID          string   `json:"latest_threadId,omitempty"`
	FullRefreshCommittedIDs []string `json:"full_refresh_committed_ids,omitempty"`
}
