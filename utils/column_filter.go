package utils

// FilterDataBySelectedColumns filters a data map to only include selected columns.
// Used for filtering CDC/WAL data during ingestion.
// Returns the original data if all columns are selected or no selection is provided.
func FilterDataBySelectedColumns(data map[string]interface{}, selectedMap map[string]struct{}, allSelected bool) map[string]interface{} {
	if len(selectedMap) == 0 {
		return data
	}
	if allSelected {
		return data
	}

	filtered := make(map[string]interface{})
	for key, value := range data {
		if _, exists := selectedMap[key]; exists {
			filtered[key] = value
		}
	}
	return filtered
}
