package utils

import "github.com/datazip-inc/olake/constants"

// binarySupportStateVersion is the first state version that keeps binary values as bytes rather than text.
// Older state keeps them as text so an existing destination column does not change type on upgrade.
const binarySupportStateVersion = 8

// IsBinarySupported reports whether the loaded state (version 8 on) keeps binary values as bytes rather than text
func IsBinarySupported() bool {
	return constants.LoadedStateVersion >= binarySupportStateVersion
}
