package typeutils

import "github.com/datazip-inc/olake/utils"

func Compare(a, b any) int {
	// Handle nil cases first
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aTime, aOk := a.(Time)
	bTime, bOk := b.(Time)

	if aOk && bOk {
		return aTime.Compare(bTime)
	}
	return utils.CompareInterfaceValue(a, b)
}
