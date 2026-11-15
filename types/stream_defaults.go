package types

import "slices"

// ApplyDestinationDefaults is the single place discover sets a stream's destination-dependent
// defaults. Source restrictions are already folded into available and writeModes; this stamps each
// stream with the delete formats (available, see AvailableUpdateTypes) and write modes (writeModes,
// see AvailableWriteModes) it may use, and picks the ones it starts on.
func ApplyDestinationDefaults(streams []*Stream, available []UpdateType, writeModes []WriteMode) {
	for _, stream := range streams {
		stream.AvailableUpdateTypes = available
		stream.AvailableWriteModes = writeModes
		defaults := stream.DefaultStreamProperties
		if defaults == nil {
			continue
		}

		defaults.UpdateType = PreferredUpdateType(available)
		// Lets the UI tell an append-only destination from "no intersection"; both list none.
		defaults.AppendMode = defaults.AppendMode || !slices.Contains(writeModes, WriteModeUpsert)
	}
}
