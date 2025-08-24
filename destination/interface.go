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

package destination

import (
	"context"
	"time"

	"github.com/datazip-inc/olake/types"
)

type Config interface {
	Validate() error
}

type Write = func(ctx context.Context, channel <-chan types.Record) error
type FlattenFunction = func(record types.Record) (types.Record, error)

type Writer interface {
	GetConfigRef() Config
	Spec() any
	Type() string
	// Sets up connections and perform checks; doesn't load Streams
	//
	// Note: Check shouldn't be called before Setup as they're composed at Connector level
	Check(ctx context.Context) error
	// Setup sets up an Adapter for dedicated use for a stream
	// avoiding the headover for different streams
	Setup(stream types.StreamInterface, opts *Options) error
	// Write function being used by drivers
	Write(ctx context.Context, record types.RawRecord) error

	// ReInitiationRequiredOnSchemaEvolution is implemented by Writers incase the writer needs to be re-initialized
	// such as when writing parquet files, but in destinations like Kafka/Clickhouse/BigQuery they can handle
	// schema update with an Alter Query
	Flattener() FlattenFunction
	// EvolveSchema updates the schema based on changes.
	// Need to pass olakeTimestamp as end argument to get the correct partition path based on record ingestion time.
	EvolveSchema(bool, bool, map[string]*types.Property, types.Record, time.Time) error
	// DropStreams is used to clear the destination before re-writing the stream
	DropStreams(ctx context.Context, selectedStream []string) error
	Close(ctx context.Context) error
}
