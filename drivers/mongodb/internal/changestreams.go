package driver

import (
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
)

func (m *Mongo) GroupRead(channel chan<- types.Record, streams ...protocol.Stream) error {
	return nil
}

func (m *Mongo) SetupGlobalState(state *types.State) error {
	return nil
}

func (m *Mongo) StateType() types.StateType {
	return ""
}
