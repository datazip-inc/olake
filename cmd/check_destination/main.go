package main

import (
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	_ "github.com/datazip-inc/olake/writers/iceberg"
	_ "github.com/datazip-inc/olake/writers/parquet"
)

// DummyDriver is a minimal implementation of protocol.Driver for check-destination command
type DummyDriver struct{}

func (d *DummyDriver) GetConfigRef() protocol.Config                    { return nil }
func (d *DummyDriver) Spec() any                                        { return nil }
func (d *DummyDriver) Check() error                                     { return nil }
func (d *DummyDriver) Type() string                                     { return "dummy" }
func (d *DummyDriver) Setup() error                                     { return nil }
func (d *DummyDriver) Discover(bool) ([]*types.Stream, error)           { return nil, nil }
func (d *DummyDriver) Read(*protocol.WriterPool, protocol.Stream) error { return nil }
func (d *DummyDriver) ChangeStreamSupported() bool                      { return false }
func (d *DummyDriver) SetupState(*types.State)                          {}

func main() {
	cmd := protocol.CreateRootCommand(true, &DummyDriver{})
	cmd.Execute()
}
