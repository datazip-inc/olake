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

// syncedKeysField is a companion state entry to the LastModified cursor. It
// holds the file keys already synced whose LastModified equals the cursor's
// second. S3 (and the S3 List API in general) only exposes LastModified at
// whole-second granularity, so a same-second collision window is up to a full
// second wide and cannot be disambiguated by timestamp alone. Remembering the
// keys already read at that second lets us use a `>=` filter and still read
// each file exactly once. See issue #1131.
const syncedKeysField = "_last_modified_time_synced_keys"

// syncedKeysFromState reconstructs the synced-key set from state. The value is
// stored as []string in-process but round-trips through JSON as []any across
// runs, so both are handled. A missing value (legacy state written before this
// fix) yields an empty set, which degrades to reading same-second boundary
// files once on the first post-upgrade sync.
func syncedKeysFromState(v any) map[string]bool {
	set := map[string]bool{}
	switch keys := v.(type) {
	case []string:
		for _, k := range keys {
			set[k] = true
		}
	case []any:
		for _, k := range keys {
			if s, ok := k.(string); ok {
				set[s] = true
			}
		}
	}
	return set
}

// keysAtSecond returns the keys of files whose LastModified equals second.
func keysAtSecond(files []FileObject, second string) []string {
	var keys []string
	for _, file := range files {
		if file.LastModified == second {
			keys = append(keys, file.FileKey)
		}
	}
	return keys
}

// FetchMaxCursorValues returns the maximum LastModified timestamp for all files in the stream
// This is used by the abstract layer to track incremental sync progress
func (s *S3) FetchMaxCursorValues(_ context.Context, stream types.StreamInterface) (any, any, error) {
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

	// Seed the synced-key set with every file at the max second. Backfill reads
	// all discovered files, so those keys are covered once backfill completes;
	// this stops the first incremental sync from re-reading them.
	if configuredStream, ok := stream.(*types.ConfiguredStream); ok && s.state != nil {
		s.state.SetCursor(configuredStream, syncedKeysField, keysAtSecond(files, maxLastModified))
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

	// Keys already synced at the cursor's second, so a same-second arrival that
	// has not been read yet is still picked up while an already-read one is not.
	syncedKeys := syncedKeysFromState(s.state.GetCursor(configuredStream, syncedKeysField))

	// Filter files to those strictly after the cursor, plus same-second files
	// not already synced.
	incrementalFiles := s.filterFilesByCursor(files, cursor, syncedKeys)

	if len(incrementalFiles) == 0 {
		logger.Infof("Stream %s: no new files to process (all already synced up to cursor)", streamName)
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
			return fmt.Errorf("failed to process incremental file %s: %w", file.FileKey, err)
		}
	}

	// Advance the synced-key set to match the new cursor second. The abstract
	// layer advances the cursor itself from record data; this keeps the set of
	// "already read at that second" keys consistent so the next sync does not
	// re-read them (and does not skip a newer same-second arrival).
	newCursor := cursor
	for _, file := range incrementalFiles {
		if file.LastModified > newCursor {
			newCursor = file.LastModified
		}
	}
	newSet := map[string]bool{}
	if newCursor == cursor {
		// The cursor second did not advance, so keys read at it earlier still count.
		for k := range syncedKeys {
			newSet[k] = true
		}
	}
	for _, file := range incrementalFiles {
		if file.LastModified == newCursor {
			newSet[file.FileKey] = true
		}
	}
	keys := make([]string, 0, len(newSet))
	for k := range newSet {
		keys = append(keys, k)
	}
	s.state.SetCursor(configuredStream, syncedKeysField, keys)

	logger.Infof("Stream %s: completed incremental processing of %d file(s)",
		streamName, len(incrementalFiles))

	return nil
}

// filterFilesByCursor returns files that still need to be read: those strictly
// newer than the cursor, plus files stamped exactly at the cursor's second that
// have not already been synced (tracked in syncedKeys). Because LastModified is
// only second-granular, a strict `>` would permanently skip a file that arrived
// in the same second as the cursor; the same-second-plus-key-set check closes
// that window while still reading each file exactly once. See issue #1131.
// The cursor timestamp is in ISO 8601 format (2006-01-02T15:04:05Z) and uses
// string comparison which works correctly for this format.
func (s *S3) filterFilesByCursor(files []FileObject, cursorTimestamp string, syncedKeys map[string]bool) []FileObject {
	var filteredFiles []FileObject
	for _, file := range files {
		switch {
		case file.LastModified > cursorTimestamp:
			filteredFiles = append(filteredFiles, file)
			logger.Debugf("File %s will be processed (modified: %s > cursor: %s)",
				file.FileKey, file.LastModified, cursorTimestamp)
		case file.LastModified == cursorTimestamp && !syncedKeys[file.FileKey]:
			filteredFiles = append(filteredFiles, file)
			logger.Debugf("File %s will be processed (same-second arrival not yet synced: %s == cursor: %s)",
				file.FileKey, file.LastModified, cursorTimestamp)
		default:
			logger.Debugf("File %s skipped (already synced up to cursor: %s <= %s)",
				file.FileKey, file.LastModified, cursorTimestamp)
		}
	}

	logger.Infof("Filtered %d files to process out of %d total (cursor: %s)",
		len(filteredFiles), len(files), cursorTimestamp)
	return filteredFiles
}
