package base

import (
	"sync"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
)

type Driver struct {
	cachedStreams sync.Map // locally cached streams; It contains all streams
	GroupRead     bool     // Used in CDC mode
}

func (d *Driver) ChangeStreamSupported() bool {
	return d.GroupRead
}

// Returns all the possible streams available in the source
func (d *Driver) GetStreams() []*types.Stream {
	streams := []*types.Stream{}
	d.cachedStreams.Range(func(key, value any) bool {
		streams = append(streams, value.(*types.Stream))

		return true
	})

	return streams
}

func (d *Driver) AddStream(stream *types.Stream) {
	d.cachedStreams.Store(stream.ID(), stream)
}

func (d *Driver) GetStream(streamID string) (bool, *types.Stream) {
	val, found := d.cachedStreams.Load(streamID)
	if !found {
		return found, nil
	}

	return found, val.(*types.Stream)
}

func (d *Driver) UpdateStateCursor(stream protocol.Stream, data types.Record) error {
	datatype, err := stream.Schema().GetType(stream.Cursor())
	if err != nil {
		return err
	}

	if cursorVal, found := data[stream.Cursor()]; found && cursorVal != nil {
		// compare with current state
		if stream.GetStateCursor() != nil {
			state, err := typeutils.MaximumOnDataType(datatype, stream.GetStateCursor(), cursorVal)
			if err != nil {
				return err
			}

			stream.SetStateCursor(state)
		} else {
			// directly update
			stream.SetStateCursor(cursorVal)
		}
	}

	return nil
}

func NewBase() *Driver {
	return &Driver{
		cachedStreams: sync.Map{},
		GroupRead:     false,
	}
}
