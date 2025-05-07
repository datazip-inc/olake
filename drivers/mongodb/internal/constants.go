package driver

const (
	cursorLastTS = "last_ts"
	cursorLastID = "last_id"
)

type IncrementalStrategy string

const (
	StrategyChangeStream IncrementalStrategy = "change_stream"
	StrategyTimestamp    IncrementalStrategy = "timestamp"
	StrategyObjectID     IncrementalStrategy = "objectid"
	StrategySoftDelete   IncrementalStrategy = "soft_delete"
)
