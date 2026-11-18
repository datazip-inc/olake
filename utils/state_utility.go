package utils

import "github.com/datazip-inc/olake/constants"

// binarySupportStateVersion is the first state version that keeps binary values as bytes rather than text.
// Older state keeps them as text so an existing destination column does not change type on upgrade.
const binarySupportStateVersion = 8

// IsBinarySupported reports whether the loaded state (version 8 on) keeps binary values as bytes rather than text
func IsBinarySupported() bool {
	return constants.LoadedStateVersion >= binarySupportStateVersion
}

// decimalSupportStateVersion is the first state version that maps exact decimal columns to decimal(p,s).
// Older state keeps them as double so an existing destination column does not change type on upgrade.
const decimalSupportStateVersion = 9

// IsDecimalSupported reports whether the loaded state (version 9 on) maps exact decimal columns to decimal(p,s) rather than double
func IsDecimalSupported() bool {
	return constants.LoadedStateVersion >= decimalSupportStateVersion
}
