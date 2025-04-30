package base

import (
	"sync"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
)

const (
	DefaultRetryCount  = 3
	DefaultThreadCount = 3
)

type Driver struct {
	cachedStreams sync.Map // locally cached streams; It contains all streams
	CDCSupport    bool     // Used in CDC mode
	State         *types.State
}

var DefaultColumns = map[string]types.DataType{
	constants.OlakeID:        types.String,
	constants.OlakeTimestamp: types.Int64,
	constants.OpType:         types.String,
	constants.CdcTimestamp:   types.Int64,
}

func (d *Driver) ChangeStreamSupported() bool {
	return d.CDCSupport
}

// Returns all the possible streams available in the source
func (d *Driver) GetStreams() []*types.Stream {
	streams := []*types.Stream{}
	d.cachedStreams.Range(func(_, value any) bool {
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

func NewBase() *Driver {
	return &Driver{
		cachedStreams: sync.Map{},
		CDCSupport:    false,
	}
}
