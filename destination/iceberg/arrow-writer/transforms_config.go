package arrowwriter

import "encoding/json"

// TransformConfig defines configuration for message transforms
type TransformConfig struct {
	// RowFilters - conditions for skipping entire rows (if true, row is SKIPPED)
	RowFilters []RowFilter `json:"row_filters,omitempty"`
	// ComputedColumns - new columns computed from existing columns
	ComputedColumns []ComputedColumn `json:"computed_columns,omitempty"`
}

// RowFilter defines a condition for filtering rows
type RowFilter struct {
	Column    string `json:"column"`    // Column to evaluate
	Condition string `json:"condition"` // e.g. "== null", "== 'deleted'", "< 0", "> 100"
}

// ComputedColumn defines a computed column
type ComputedColumn struct {
	Name       string `json:"name"`       // New column name
	Expression string `json:"expression"` // e.g. "col1 + col2", "UPPER(col)", "CONCAT(a, b)"
	Type       string `json:"type"`       // "string", "int", "long", "double", "boolean"
}

// ParseTransformConfigFromJSON parses config from JSON
func ParseTransformConfigFromJSON(jsonStr string) (*TransformConfig, error) {
	if jsonStr == "" {
		return nil, nil
	}
	var config TransformConfig
	if err := json.Unmarshal([]byte(jsonStr), &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// GetComputedColumnsSchema returns computed columns as schema map (col_name -> iceberg_type)
// Use this to include computed columns when evolving Iceberg schema
func (c *TransformConfig) GetComputedColumnsSchema() map[string]string {
	if c == nil {
		return nil
	}
	schema := make(map[string]string)
	for _, cc := range c.ComputedColumns {
		schema[cc.Name] = cc.Type
	}
	return schema
}
