package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// ============================================
// Incremental Sync: Cursor-Based Processing
// ============================================
// This file contains all incremental sync logic using cursor-based filtering

// FetchMaxCursorValues returns the maximum LastModified timestamp for all files in the stream
// This is used by the abstract layer to track incremental sync progress
func (s *S3) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	streamName := stream.Name()

	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		logger.Debugf("No files found for stream %s, returning nil cursor", streamName)
		return nil, nil, nil
	}

	// Find the latest LastModified timestamp among all files in this stream
	var maxLastModified string
	for _, file := range files {
		if file.LastModified > maxLastModified {
			maxLastModified = file.LastModified
		}
	}

	logger.Infof("Max cursor value for stream %s: %s (from %d files)", streamName, maxLastModified, len(files))

	// Return as primary cursor (secondary cursor not used for S3)
	return maxLastModified, nil, nil
}

// StreamIncrementalChanges processes files that were added/modified after backfill completed
// This follows olake's incremental sync architecture:
//  1. Backfill phase processes all discovered files
//  2. This method processes only files added/modified AFTER the cursor (LastModified > cursor)
//  3. Processes files sequentially (no chunking/parallelization for simplicity)
func (s *S3) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	streamName := stream.Name()

	// Get the cursor from the abstract layer's state management
	configuredStream, ok := stream.(*types.ConfiguredStream)
	if !ok {
		logger.Infof("Stream %s: not a configured stream, skipping incremental sync", streamName)
		return nil
	}

	// Get primary cursor field
	primaryCursor, _ := configuredStream.Cursor()
	if primaryCursor == "" {
		logger.Infof("Stream %s: no cursor configured, skipping incremental sync", streamName)
		return nil
	}

	// Get cursor value from state (managed by abstract layer)
	cursorValue := s.state.GetCursor(configuredStream, primaryCursor)
	if cursorValue == nil {
		logger.Infof("Stream %s: no cursor value in state, skipping incremental sync", streamName)
		return nil
	}

	// Parse cursor as string (timestamp format: 2006-01-02T15:04:05Z)
	cursor, ok := cursorValue.(string)
	if !ok {
		logger.Warnf("Stream %s: invalid cursor format, expected string got %T", streamName, cursorValue)
		return nil
	}

	logger.Infof("Stream %s: processing incremental changes (files with LastModified > %s)",
		streamName, cursor)

	// Get all discovered files for this stream
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		logger.Infof("Stream %s: no files found for incremental processing", streamName)
		return nil
	}

	// Filter files to only include those modified AFTER the cursor
	incrementalFiles := s.filterFilesByCursor(files, cursor)

	if len(incrementalFiles) == 0 {
		logger.Infof("Stream %s: no new files to process (all files <= cursor)", streamName)
		return nil
	}

	logger.Infof("Stream %s: found %d file(s) for incremental processing",
		streamName, len(incrementalFiles))

	// Process each incremental file sequentially
	// Note: We process one-by-one for simplicity. Parallelization can be added later if needed.
	for i, file := range incrementalFiles {
		logger.Infof("Stream %s: processing incremental file %d/%d: %s (LastModified: %s)",
			streamName, i+1, len(incrementalFiles), file.FileKey, file.LastModified)

		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Process the file using the same logic as backfill
		// Pass file.LastModified directly (already available) to avoid redundant lookup
		if err := s.processFile(ctx, stream, file.FileKey, file.Size, file.LastModified, cb); err != nil {
			return fmt.Errorf("failed to process incremental file %s: %s", file.FileKey, err)
		}
	}

	logger.Infof("Stream %s: completed incremental processing of %d file(s)",
		streamName, len(incrementalFiles))

	return nil
}

// filterFilesByCursor filters files based on LastModified timestamp cursor
// Returns only files where LastModified > cursor
// The cursor timestamp is in ISO 8601 format (2006-01-02T15:04:05Z) and uses
// string comparison which works correctly for this format.
func (s *S3) filterFilesByCursor(files []FileObject, cursorTimestamp string) []FileObject {
	var filteredFiles []FileObject
	for _, file := range files {
		if file.LastModified > cursorTimestamp {
			filteredFiles = append(filteredFiles, file)
			logger.Debugf("File %s will be processed (modified: %s > cursor: %s)",
				file.FileKey, file.LastModified, cursorTimestamp)
		} else {
			logger.Debugf("File %s skipped (modified: %s <= cursor: %s)",
				file.FileKey, file.LastModified, cursorTimestamp)
		}
	}

	logger.Infof("Filtered %d files to process out of %d total (cursor: %s)",
		len(filteredFiles), len(files), cursorTimestamp)
	return filteredFiles
}
