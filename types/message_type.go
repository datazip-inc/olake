package types

type MessageType string

const (
	LogMessage              MessageType = "LOG"
	ConnectionStatusMessage MessageType = "CONNECTION_STATUS"
	StateMessage            MessageType = "STATE"
	RecordMessage           MessageType = "RECORD"
	CatalogMessage          MessageType = "CATALOG"
	SpecMessage             MessageType = "SPEC"
	ActionMessage           MessageType = "ACTION"
)

type ConnectionStatus string

const (
	ConnectionSucceed ConnectionStatus = "SUCCEEDED"
	ConnectionFailed  ConnectionStatus = "FAILED"
)
