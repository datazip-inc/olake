package base

import (
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// Pass dest with all fields initialized to handle nil state case
func ManageGlobalState[T any](state *types.State, dest *T, driver protocol.ChangeStreamDriver) error {
	state.Type = driver.StateType()

	if state.Global != nil {
		err := utils.Unmarshal(state.Global, dest)
		if err != nil {
			return err
		}
	}

	// set Global State reference
	state.Global = dest

	return nil
}
