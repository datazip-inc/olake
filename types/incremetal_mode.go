package types

type IncrementalStrategy string

const (
	StrategyTimestamp  IncrementalStrategy = "timestamp"
	StrategyObjectID   IncrementalStrategy = "objectid"
	StrategySoftDelete IncrementalStrategy = "soft_delete"
)
