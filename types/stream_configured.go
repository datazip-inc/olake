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

import (
	"fmt"
	"regexp"
	"strings"
)

// Input/Processed object for Stream
type ConfiguredStream struct {
	StreamMetadata StreamMetadata `json:"-"`
	Stream         *Stream        `json:"stream,omitempty"`
}

// Condition represents a single condition in a filter
type Condition struct {
	Column   string
	Operator string
	Value    string
}

// Filter represents the parsed filter
type Filter struct {
	Conditions      []Condition // a > b, a < b
	LogicalOperator string      // condition[0] and/or condition[1], single and/or supported
}

func (s *ConfiguredStream) ID() string {
	return s.Stream.ID()
}

func (s *ConfiguredStream) Self() *ConfiguredStream {
	return s
}

func (s *ConfiguredStream) Name() string {
	return s.Stream.Name
}

func (s *ConfiguredStream) GetStream() *Stream {
	return s.Stream
}

func (s *ConfiguredStream) Namespace() string {
	return s.Stream.Namespace
}

func (s *ConfiguredStream) Schema() *TypeSchema {
	return s.Stream.Schema
}

func (s *ConfiguredStream) SupportedSyncModes() *Set[SyncMode] {
	return s.Stream.SupportedSyncModes
}

func (s *ConfiguredStream) GetSyncMode() SyncMode {
	return s.Stream.SyncMode
}

// returns primary and secondary cursor
func (s *ConfiguredStream) Cursor() (string, string) {
	cursorFields := strings.Split(s.Stream.CursorField, ":")
	primaryCursor := cursorFields[0]
	secondaryCursor := ""
	if len(cursorFields) > 1 {
		secondaryCursor = cursorFields[1]
	}
	return primaryCursor, secondaryCursor
}

func (s *ConfiguredStream) GetFilter() (Filter, error) {
	filter := s.StreamMetadata.Filter
	if filter == "" {
		return Filter{}, nil
	}
	// example: a>b, a>=b, a<b, a<=b, a!=b, a=b, a="b", a=\"b\" and c>d, a="b" or c>d
	var FilterRegex = regexp.MustCompile(`^(\w+)\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\d*\.?\d+|\w+)\s*(?:(and|or)\s*(\w+)\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\d*\.?\d+|\w+))?\s*$`)
	matches := FilterRegex.FindStringSubmatch(filter)
	if len(matches) == 0 {
		return Filter{}, fmt.Errorf("invalid filter format: %s", filter)
	}
	var conditions []Condition
	conditions = append(conditions, Condition{
		Column:   matches[1],
		Operator: matches[2],
		Value:    matches[3],
	})

	if matches[4] != "" {
		conditions = append(conditions, Condition{
			Column:   matches[5],
			Operator: matches[6],
			Value:    matches[7],
		})
	}

	return Filter{
		Conditions:      conditions,
		LogicalOperator: matches[4],
	}, nil
}

// Validate Configured Stream with Source Stream
func (s *ConfiguredStream) Validate(source *Stream) error {
	if !source.SupportedSyncModes.Exists(s.Stream.SyncMode) {
		return fmt.Errorf("invalid sync mode[%s]; valid are %v", s.Stream.SyncMode, source.SupportedSyncModes)
	}

	// no cursor validation in cdc and backfill sync
	if s.Stream.SyncMode == INCREMENTAL {
		primaryCursor, secondaryCursor := s.Cursor()
		if !source.AvailableCursorFields.Exists(primaryCursor) {
			return fmt.Errorf("invalid cursor field [%s]; valid are %v", primaryCursor, source.AvailableCursorFields)
		}
		if secondaryCursor != "" && !source.AvailableCursorFields.Exists(secondaryCursor) {
			return fmt.Errorf("invalid secondary cursor field [%s]; valid are %v", secondaryCursor, source.AvailableCursorFields)
		}
	}

	if source.SourceDefinedPrimaryKey.ProperSubsetOf(s.Stream.SourceDefinedPrimaryKey) {
		return fmt.Errorf("differnce found with primary keys: %v", source.SourceDefinedPrimaryKey.Difference(s.Stream.SourceDefinedPrimaryKey).Array())
	}

	_, err := s.GetFilter()
	if err != nil {
		return fmt.Errorf("failed to parse filter %s: %s", s.StreamMetadata.Filter, err)
	}

	return nil
}

func (s *ConfiguredStream) NormalizationEnabled() bool {
	return s.StreamMetadata.Normalization
}
