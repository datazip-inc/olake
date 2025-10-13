package types

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

type StreamCategories struct {
	SelectedStreams    []string
	CDCStreams         []StreamInterface
	IncrementalStreams []StreamInterface
	StandardStreams    []StreamInterface
	NewStreamsState    []*StreamState
}

func IdentifySelectedStreams(catalog *Catalog, streams []*Stream, state *State) (*StreamCategories, error) {
	categories := &StreamCategories{
		SelectedStreams:    []string{},
		CDCStreams:         []StreamInterface{},
		IncrementalStreams: []StreamInterface{},
		StandardStreams:    []StreamInterface{},
		NewStreamsState:    []*StreamState{},
	}
	// create a map for namespace and streamMetadata
	selectedStreamsMap := make(map[string]StreamMetadata)
	for namespace, streamsMetadata := range catalog.SelectedStreams {
		for _, streamMetadata := range streamsMetadata {
			selectedStreamsMap[fmt.Sprintf("%s.%s", namespace, streamMetadata.StreamName)] = streamMetadata
		}
	}

	// Create a map for quick state lookup by stream ID
	stateStreamMap := make(map[string]*StreamState)
	for _, stream := range state.Streams {
		stateStreamMap[fmt.Sprintf("%s.%s", stream.Namespace, stream.Stream)] = stream
	}

	_, _ = utils.ArrayContains(catalog.Streams, func(elem *ConfiguredStream) bool {
		streamID := fmt.Sprintf("%s.%s", elem.Namespace(), elem.Name())
		sMetadata, selected := selectedStreamsMap[streamID]
		// Check if the stream is in the selectedStreamMap
		if !(catalog.SelectedStreams == nil || selected) {
			logger.Debugf("Skipping stream %s.%s; not in selected streams.", elem.Namespace(), elem.Name())
			return false
		}

		if streams != nil {
			source, found := StreamsToMap(streams...)[elem.ID()]
			if !found {
				logger.Warnf("Skipping; Configured Stream %s not found in source", elem.ID())
				return false
			}
			elem.StreamMetadata = sMetadata
			err := elem.Validate(source)
			if err != nil {
				logger.Warnf("Skipping; Configured Stream %s found invalid due to reason: %s", elem.ID(), err)
				return false
			}
		}

		categories.SelectedStreams = append(categories.SelectedStreams, elem.ID())
		switch elem.Stream.SyncMode {
		case CDC, STRICTCDC:
			categories.CDCStreams = append(categories.CDCStreams, elem)
			streamState, exists := stateStreamMap[streamID]
			if exists {
				categories.NewStreamsState = append(categories.NewStreamsState, streamState)
			}
		case INCREMENTAL:
			categories.IncrementalStreams = append(categories.IncrementalStreams, elem)
			streamState, exists := stateStreamMap[streamID]
			if exists {
				categories.NewStreamsState = append(categories.NewStreamsState, streamState)
			}
		default:
			categories.StandardStreams = append(categories.StandardStreams, elem)
		}

		return false
	})
	// This removes all the previous state streams (clean-up of previous state).
	// for non-selected streams, both global and stream state must be retained during clear destination.
	// stream == nil marks clear destination is called.
	if streams != nil {
		state.Streams = categories.NewStreamsState
	}
	if len(categories.SelectedStreams) == 0 {
		return nil, fmt.Errorf("no valid streams found in catalog")
	}

	logger.Infof("Valid selected streams are %s", strings.Join(categories.SelectedStreams, ", "))
	return categories, nil
}
