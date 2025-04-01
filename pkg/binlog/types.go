package binlog

import (
	"time"

	"github.com/datazip-inc/olake/protocol"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// Config holds the configuration for the binlog syncer.
type Config struct {
	ServerID        uint32
	Flavor          string
	Host            string
	Port            uint16
	User            string
	Password        string
	Charset         string
	VerifyChecksum  bool
	HeartbeatPeriod time.Duration
	InitialWaitTime time.Duration
}

// BinlogState holds the current binlog position.
type Binlog struct {
	Position mysql.Position `json:"position"`
}

// CDCChange represents a change event captured from the binlog.
type CDCChange struct {
	Stream    protocol.Stream
	Timestamp time.Time
	Position  mysql.Position
	Kind      string
	Schema    string
	Table     string
	Data      map[string]interface{}
}

// OnChange is a callback function type for processing CDC changes.
type OnChange func(change CDCChange) error
