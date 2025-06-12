package waljs

import (
	"crypto/tls"
	"net/url"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
)

type Config struct {
	Tables              *types.Set[types.StreamInterface]
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
	SlotType   string        `db:"slot_type"`
	Plugin     string        `db:"plugin"`
	LSN        pglogrepl.LSN `db:"confirmed_flush_lsn"`
	CurrentLSN pglogrepl.LSN `db:"current_lsn"`
}

type WALMessage struct {
	NextLSN   string         `json:"nextlsn"`
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
