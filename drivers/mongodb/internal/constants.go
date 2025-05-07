package driver

const (
	cursorLastTS = "last_ts"
	cursorLastID = "last_id"
)

type IncrementalStrategy string

const (
	StrategyChangeStream IncrementalStrategy = "change_stream" // default – existing behaviour
	StrategyTimestamp    IncrementalStrategy = "timestamp"     // new
	StrategyObjectID     IncrementalStrategy = "objectid"      // new
	StrategySoftDelete   IncrementalStrategy = "soft_delete"   // new
)
