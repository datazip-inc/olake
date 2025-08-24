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

type MessageType string

const (
	LogMessage              MessageType = "LOG"
	ConnectionStatusMessage MessageType = "CONNECTION_STATUS"
	StateMessage            MessageType = "STATE"
	RecordMessage           MessageType = "RECORD"
	CatalogMessage          MessageType = "CATALOG"
	SpecMessage             MessageType = "SPEC"
	ActionMessage           MessageType = "ACTION"
)

type ConnectionStatus string

const (
	ConnectionSucceed ConnectionStatus = "SUCCEEDED"
	ConnectionFailed  ConnectionStatus = "FAILED"
)
