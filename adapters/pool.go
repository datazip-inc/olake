package adapters

import (
	"fmt"
	"sync/atomic"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"golang.org/x/sync/errgroup"
)

type TestFunc func(config any) error
type NewFunc func(config any) (protocol.Adapter, error)

var RegisteredAdapters = map[types.AdapterType]struct {
	New  NewFunc
	Test TestFunc
}{}

type WritePool struct {
	recordCount   atomic.Int64
	threadCounter atomic.Int64 // Used in naming files in S3 and global count for threads
	config        any          // respective adapter config
	init          NewFunc      // To initialize exclusive destination threads
	group         errgroup.Group
}

type Thread struct {
	channel chan types.Record
}

func NewWriter(config types.WriterConfig) (*WritePool, error) {
	adapter, found := RegisteredAdapters[config.Type]
	if !found {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}
	err := adapter.Test(config)
	if err != nil {
		return nil, fmt.Errorf("failed to test destination: %s", err)
	}

	return &WritePool{
		recordCount:   atomic.Int64{},
		threadCounter: atomic.Int64{},
		config:        config.AdapterConfig,
		init:          adapter.New,
	}, nil
}

// Initialize new adapter thread for writing into destination
func (w *WritePool) NewThread() (protocol.Adapter, error) {
	adapter, err := w.init(w.config)
	if err != nil {
		return nil, err
	}

}

// Returns total records fetched at runtime
func (w *WritePool) TotalRecords() int64 {
	return w.recordCount.Load()
}
