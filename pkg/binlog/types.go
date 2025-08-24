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

package binlog

import (
	"time"

	"github.com/datazip-inc/olake/types"
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
	Stream    types.StreamInterface
	Timestamp time.Time
	Position  mysql.Position
	Kind      string
	Schema    string
	Table     string
	Data      map[string]interface{}
}

// OnChange is a callback function type for processing CDC changes.
type OnChange func(change CDCChange) error
