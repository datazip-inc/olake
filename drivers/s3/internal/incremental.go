package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// FetchMaxCursorValues returns the maximum LastModified timestamp for all files in the stream
func (s *S3) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	streamName := stream.Name()
	
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		return nil, nil, nil
	}

	// Find the latest LastModified timestamp among all files in this stream
	var maxLastModified string
	for _, file := range files {
		if file.LastModified > maxLastModified {
			maxLastModified = file.LastModified
		}
	}

	// Return as primary cursor (secondary cursor not used for S3)
	return maxLastModified, nil, nil
}

// StreamIncrementalChanges processes only new or modified files since last sync
// Uses parallel chunk processing for better performance
func (s *S3) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	streamName := stream.Name()
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		return fmt.Errorf("no files found for stream: %s", streamName)
	}

	logger.Infof("Checking for incremental changes for stream: %s (%d files)", streamName, len(files))

	// Filter files based on LastModified cursor
	filesToSync, err := s.filterFilesForIncremental(stream, files)
	if err != nil {
		return fmt.Errorf("failed to filter files: %w", err)
	}

	if len(filesToSync) == 0 {
		logger.Infof("No new or modified files for stream %s", streamName)
		return nil
	}

	logger.Infof("Found %d files to sync for stream %s (out of %d total)", 
		len(filesToSync), streamName, len(files))

	// Process filtered files using parallel chunk processing
	return s.processFilesInParallel(ctx, stream, filesToSync, cb)
}

// filterFilesForIncremental filters files based on LastModified cursor from state
func (s *S3) filterFilesForIncremental(stream types.StreamInterface, files []FileObject) ([]FileObject, error) {
	// Get configured stream to access state
	configuredStream, ok := stream.(*types.ConfiguredStream)
	if !ok {
		logger.Warnf("Failed to cast to ConfiguredStream, syncing all files")
		return files, nil
	}

	// Get last synced timestamp from state (primary cursor)
	primaryCursor, _ := configuredStream.Cursor()
	lastSyncedTimestamp := s.state.GetCursor(configuredStream, primaryCursor)

	if lastSyncedTimestamp == nil {
		// First sync - process all files
		logger.Infof("No previous state for stream %s, syncing all %d files", stream.Name(), len(files))
		return files, nil
	}

	// Parse last synced timestamp
	lastSynced, ok := lastSyncedTimestamp.(string)
	if !ok {
		logger.Warnf("Invalid cursor format in state for stream %s, syncing all files", stream.Name())
		return files, nil
	}

	// Filter files modified after last sync
	var filesToSync []FileObject
	for _, file := range files {
		if file.LastModified > lastSynced {
			filesToSync = append(filesToSync, file)
			logger.Debugf("File %s modified (%s > %s), will sync", file.FileKey, file.LastModified, lastSynced)
		}
	}

	return filesToSync, nil
}

// processFilesInParallel processes files using parallel goroutines with worker pool pattern
func (s *S3) processFilesInParallel(ctx context.Context, stream types.StreamInterface, files []FileObject, cb abstract.BackfillMsgFn) error {
	logger.Infof("Processing %d files in parallel for stream %s", len(files), stream.Name())

	// Create error channel to collect errors from workers
	errChan := make(chan error, len(files))
	
	// Use semaphore pattern to limit concurrent file processing
	// This prevents overwhelming S3 API or consuming too much memory
	maxConcurrency := s.config.MaxThreads
	if maxConcurrency <= 0 {
		maxConcurrency = 4 // Default concurrency
	}
	semaphore := make(chan struct{}, maxConcurrency)

	// Process files concurrently
	for i, file := range files {
		// Acquire semaphore
		semaphore <- struct{}{}
		
		go func(fileIndex int, fileObj FileObject) {
			defer func() { <-semaphore }() // Release semaphore
			
			logger.Infof("Processing file %d/%d: %s", fileIndex+1, len(files), fileObj.FileKey)
			
			chunk := types.Chunk{
				Min: fileObj.FileKey, // File key as chunk identifier
				Max: fileObj.Size,    // File size for range requests
			}
			
			if err := s.ChunkIterator(ctx, stream, chunk, cb); err != nil {
				errChan <- fmt.Errorf("failed to process file %s: %w", fileObj.FileKey, err)
				return
			}
			
			logger.Debugf("Successfully processed file: %s", fileObj.FileKey)
			errChan <- nil
		}(i, file)
	}

	// Wait for all goroutines to complete and collect errors
	var firstErr error
	for i := 0; i < len(files); i++ {
		if err := <-errChan; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
