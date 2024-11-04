package driver

import (
	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
)

type Mongo struct {
	*base.Driver
}

// config reference; must be pointer
func (m *Mongo) Config() any {
	return nil
}

func (m *Mongo) Spec() any {
	return nil
}
func (m *Mongo) Check() error {
	return nil
}

func (m *Mongo) Setup() error {
	return nil
}

func (m *Mongo) Type() string {
	return "MongoDB"
}

func (m *Mongo) Discover() ([]*types.Stream, error) {
	return nil, nil
}

func (m *Mongo) Read(stream protocol.Stream, channel chan<- types.Record) error {
	return nil
}

func (m *Mongo) BulkRead() bool {
	return true
}
