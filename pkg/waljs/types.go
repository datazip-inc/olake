/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
