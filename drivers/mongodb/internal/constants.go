package driver

const (
	cursorLastTS = "last_ts"
	cursorLastID = "last_id"
)

type IncrementalStrategy string

const (
	StrategyTimestamp  IncrementalStrategy = "timestamp"
	StrategyObjectID   IncrementalStrategy = "objectid"
	StrategySoftDelete IncrementalStrategy = "soft_delete"
)
