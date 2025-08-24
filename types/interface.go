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

package types

type StreamInterface interface {
	ID() string
	Self() *ConfiguredStream
	Name() string
	Namespace() string
	Schema() *TypeSchema
	GetStream() *Stream
	GetSyncMode() SyncMode
	GetFilter() (Filter, error)
	SupportedSyncModes() *Set[SyncMode]
	Cursor() (string, string)
	Validate(source *Stream) error
	NormalizationEnabled() bool
}

type StateInterface interface {
	ResetStreams()
	SetType(typ StateType)
	GetCursor(stream *ConfiguredStream, key string) any
	SetCursor(stream *ConfiguredStream, key, value any)
	GetChunks(stream *ConfiguredStream) *Set[Chunk]
	SetChunks(stream *ConfiguredStream, chunks *Set[Chunk])
	RemoveChunk(stream *ConfiguredStream, chunk Chunk)
	SetGlobal(globalState any, streams ...string)
}

type Iterable interface {
	Next() bool
	Err() error
}
