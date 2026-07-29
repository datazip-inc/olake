// TODO: unify these onto lib/constants directly in a later refactor. Deferred for now because the
// change spans most of the codebase.
package constants

import (
	libconstants "github.com/datazip-inc/olake/lib/constants"
)

const LatestStateVersion = libconstants.LatestStateVersion

// Used as the current version of the state when the program is running
var LoadedStateVersion = LatestStateVersion
