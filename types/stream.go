package types

type ConfiguredStream struct {
	SyncMode    string     `json:"sync_mode"`
	CursorField string     `json:"cursor_field"`
	Stream      BaseStream `json:"stream"`
}

type BaseStream struct {
	Name                  string   `json:"name"`
	Namespace             string   `json:"namespace,omitempty"`
	AvailableCursorFields []string `json:"cursor_fields"`
	PrimaryKeys           []string `json:"primary_keys"`
}
