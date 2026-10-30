package types

// ApplyDestinationDefaults is the single place discover sets a stream's destination-dependent
// defaults. Source restrictions are already folded into available; this stamps each stream with the delete formats it
// may use (available, see AvailableUpdateTypes), picks the one it starts on, and turns on append
// mode when the destination cannot upsert (appendOnly).
func ApplyDestinationDefaults(streams []*Stream, available []UpdateType, appendOnly bool) {
	for _, stream := range streams {
		stream.AvailableUpdateTypes = available
		defaults := stream.DefaultStreamProperties
		if defaults == nil {
			continue
		}

		defaults.UpdateType = PreferredUpdateType(available)
		// Lets the UI tell an append-only destination from "no intersection"; both list none.
		defaults.AppendMode = defaults.AppendMode || appendOnly
	}
}
