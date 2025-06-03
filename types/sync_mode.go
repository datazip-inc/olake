package types

type SyncMode string

const (
	FULLREFRESH SyncMode = "full_refresh"
	INCREMENTAL SyncMode = "incremental"
	CDC         SyncMode = "cdc"
	STRICTCDC   SyncMode = "strict_cdc"
)
