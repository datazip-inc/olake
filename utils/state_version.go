package utils

import "github.com/datazip-inc/olake/constants"

// IsBinarySupported reports whether the loaded state (version 8 on) keeps binary values as bytes rather than text
func IsBinarySupported() bool {
	return constants.LoadedStateVersion >= 8
}
