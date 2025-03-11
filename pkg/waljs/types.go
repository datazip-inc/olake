package waljs

import (
	"crypto/tls"
	"net/url"
	"time"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/jackc/pglogrepl"
)

type Config struct {
	Tables              *types.Set[protocol.Stream]
	Connection          url.URL
	ReplicationSlotName string
	InitialWaitTime     time.Duration
	TLSConfig           *tls.Config
	BatchSize           int
}

type WALState struct {
	LSN string `json:"lsn"`
}

func (s *WALState) IsEmpty() bool {
	return s == nil || s.LSN == ""
}

type ReplicationSlot struct {
	SlotType string        `db:"slot_type"`
	Plugin   string        `db:"plugin"`
	LSN      pglogrepl.LSN `db:"confirmed_flush_lsn"`
}

type CDCChange struct {
	Stream    protocol.Stream
	Timestamp typeutils.Time
	LSN       pglogrepl.LSN
	Kind      string
	Schema    string
	Table     string
	Data      map[string]any
}

type WALMessage struct {
	// NextLSN   pglogrepl.LSN `json:"nextlsn"`
	Timestamp typeutils.Time `json:"timestamp"`
	Change    []struct {
		Kind         string        `json:"kind"`
		Schema       string        `json:"schema"`
		Table        string        `json:"table"`
		Columnnames  []string      `json:"columnnames"`
		Columntypes  []string      `json:"columntypes"`
		Columnvalues []interface{} `json:"columnvalues"`
		Oldkeys      struct {
			Keynames  []string      `json:"keynames"`
			Keytypes  []string      `json:"keytypes"`
			Keyvalues []interface{} `json:"keyvalues"`
		} `json:"oldkeys"`
	} `json:"change"`
}

type OnMessage = func(message CDCChange) error
