package parquet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	parquet2PCDir           = "_olake_2pc"
	parquet2PCCompletedFile = "_completed.json"
	parquet2PCStagingSuffix = ".staging"
)

type parquet2PCCompletedMarker struct {
	ThreadID      string
	MetadataState *types.MetadataState
	// completedAt selects the latest CDC/incremental metadata marker.
	completedAt time.Time
}

type parquet2PCStagingEntry struct {
	StagingDir string
	Marker     *parquet2PCCompletedMarker
}

// load2PCState restores committed progress and reconciles staged attempts before setup returns.
func (p *Parquet) load2PCState(ctx context.Context) (*types.MetadataState, error) {
	stagingEntries, err := p.listStagingEntries(ctx)
	if err != nil {
		return nil, err
	}

	markers := make([]parquet2PCCompletedMarker, 0, len(stagingEntries))
	for _, entry := range stagingEntries {
		if entry.Marker == nil {
			if err := p.deleteStagingByName(ctx, entry.StagingDir); err != nil {
				return nil, err
			}
			continue
		}

		marker := *entry.Marker
		markers = append(markers, marker)
		if err := p.promoteStaging(ctx, marker.ThreadID); err != nil {
			return nil, err
		}
		if err := p.cleanupCommittedStaging(marker.ThreadID); err != nil {
			logger.Warnf("Thread[%s]: failed to cleanup committed parquet 2pc staging dir: %s", marker.ThreadID, err)
		}
	}

	fullRefreshCommittedIDs := make([]string, 0, len(markers))
	var latestStateMarker *parquet2PCCompletedMarker
	for _, marker := range markers {
		if marker.ThreadID == "" {
			continue
		}
		if marker.MetadataState == nil {
			fullRefreshCommittedIDs = append(fullRefreshCommittedIDs, marker.ThreadID)
			continue
		}
		if latestStateMarker == nil || marker.completedAt.After(latestStateMarker.completedAt) {
			markerCopy := marker
			latestStateMarker = &markerCopy
		}
	}

	var state *types.MetadataState
	if latestStateMarker != nil {
		stateCopy := *latestStateMarker.MetadataState
		state = &stateCopy
	}
	if state == nil && len(fullRefreshCommittedIDs) == 0 {
		return state, nil
	}
	if state == nil {
		state = &types.MetadataState{}
	}

	knownIDs := make(map[string]bool, len(state.FullRefreshCommittedIDs)+len(fullRefreshCommittedIDs))
	for _, id := range state.FullRefreshCommittedIDs {
		knownIDs[id] = true
	}
	for _, id := range fullRefreshCommittedIDs {
		if !knownIDs[id] {
			state.FullRefreshCommittedIDs = append(state.FullRefreshCommittedIDs, id)
			knownIDs[id] = true
		}
	}
	return state, nil
}

// metadataState normalizes close-time state into the MetadataState shape used for recovery.
func (p *Parquet) metadataState(finalMetadataState any) (*types.MetadataState, error) {
	var metadataState *types.MetadataState
	if finalMetadataState == nil {
		return metadataState, nil
	}

	switch state := finalMetadataState.(type) {
	case *types.MetadataState:
		if state == nil {
			return metadataState, nil
		}
		stateCopy := *state
		metadataState = &stateCopy
	case types.MetadataState:
		stateCopy := state
		metadataState = &stateCopy
	default:
		stateFromPayload, err := types.SetMetadataState(finalMetadataState, p.options.ThreadID)
		if err != nil {
			return nil, err
		}
		metadataState = stateFromPayload
	}

	if metadataState.ID == nil || fmt.Sprint(metadataState.ID) == "" {
		metadataState.ID = p.options.ThreadID
	}
	return metadataState, nil
}

// writeCompletedMarker records the current thread as destination-committed.
func (p *Parquet) writeCompletedMarker(ctx context.Context, metadataState *types.MetadataState) error {
	data := []byte("{}")
	if metadataState != nil {
		var err error
		data, err = json.Marshal(metadataState)
		if err != nil {
			return fmt.Errorf("failed to marshal parquet 2pc completed marker: %s", err)
		}
	}
	return p.write2PCObject(ctx, p.completedMarkerName(p.options.ThreadID), data)
}

// listStagingEntries lists staging folders and attaches completed markers when present.
func (p *Parquet) listStagingEntries(ctx context.Context) (map[string]parquet2PCStagingEntry, error) {
	if p.s3Client != nil {
		return p.listS3StagingEntries(ctx)
	}
	return p.listLocalStagingEntries()
}

// listLocalStagingEntries builds staging entries from local 2PC directories.
func (p *Parquet) listLocalStagingEntries() (map[string]parquet2PCStagingEntry, error) {
	entries, err := os.ReadDir(p.local2PCPath())
	if os.IsNotExist(err) {
		return map[string]parquet2PCStagingEntry{}, nil
	}
	if err != nil {
		return nil, err
	}

	stagingEntries := make(map[string]parquet2PCStagingEntry)
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasSuffix(entry.Name(), parquet2PCStagingSuffix) {
			continue
		}
		stagingEntry := parquet2PCStagingEntry{StagingDir: entry.Name()}
		marker, err := p.readLocalCompletedMarker(entry.Name())
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err == nil {
			stagingEntry.Marker = &marker
		}
		stagingEntries[entry.Name()] = stagingEntry
	}
	return stagingEntries, nil
}

// listS3StagingEntries builds staging entries from objects under the table 2PC prefix.
func (p *Parquet) listS3StagingEntries(ctx context.Context) (map[string]parquet2PCStagingEntry, error) {
	prefix := p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir)) + "/"
	stagingEntries := make(map[string]parquet2PCStagingEntry)
	var pageErr error

	err := p.retryS3(ctx, func(ctx context.Context) error {
		pageErr = nil
		stagingEntries = make(map[string]parquet2PCStagingEntry)
		return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(prefix),
		}, func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, obj := range page.Contents {
				if obj.Key == nil {
					continue
				}
				relPath := strings.TrimPrefix(*obj.Key, prefix)
				parts := strings.SplitN(relPath, "/", 2)
				if len(parts) != 2 || !strings.HasSuffix(parts[0], parquet2PCStagingSuffix) {
					continue
				}

				stagingEntry := stagingEntries[parts[0]]
				if stagingEntry.StagingDir == "" {
					stagingEntry.StagingDir = parts[0]
				}
				if parts[1] == parquet2PCCompletedFile {
					marker, err := p.readS3CompletedMarker(ctx, parts[0], *obj.Key, obj.LastModified)
					if err != nil {
						pageErr = err
						return false
					}
					stagingEntry.Marker = &marker
				}
				stagingEntries[parts[0]] = stagingEntry
			}
			return true
		})
	})
	if err != nil {
		return nil, err
	}
	return stagingEntries, pageErr
}

// readLocalCompletedMarker reads and parses a local _completed.json marker.
func (p *Parquet) readLocalCompletedMarker(stagingDir string) (parquet2PCCompletedMarker, error) {
	threadID, err := threadIDFromStagingDir(stagingDir)
	if err != nil {
		return parquet2PCCompletedMarker{}, err
	}

	path := filepath.Join(p.local2PCPath(), stagingDir, parquet2PCCompletedFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return parquet2PCCompletedMarker{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return parquet2PCCompletedMarker{}, err
	}
	return p.parseCompletedMarker(threadID, data, info.ModTime())
}

// readS3CompletedMarker reads and parses an S3 _completed.json marker.
func (p *Parquet) readS3CompletedMarker(ctx context.Context, stagingDir, key string, lastModified *time.Time) (parquet2PCCompletedMarker, error) {
	threadID, err := threadIDFromStagingDir(stagingDir)
	if err != nil {
		return parquet2PCCompletedMarker{}, err
	}

	data, err := p.readS3Object(ctx, key)
	if err != nil {
		return parquet2PCCompletedMarker{}, err
	}

	completedAt := time.Time{}
	if lastModified != nil {
		completedAt = *lastModified
	}
	return p.parseCompletedMarker(threadID, data, completedAt)
}

// parseCompletedMarker decodes optional metadata from a completed marker body.
func (p *Parquet) parseCompletedMarker(threadID string, data []byte, completedAt time.Time) (parquet2PCCompletedMarker, error) {
	marker := parquet2PCCompletedMarker{
		ThreadID:    threadID,
		completedAt: completedAt,
	}

	trimmedData := bytes.TrimSpace(data)
	if len(trimmedData) == 0 || bytes.Equal(trimmedData, []byte("{}")) {
		return marker, nil
	}

	var metadataState types.MetadataState
	if err := json.Unmarshal(trimmedData, &metadataState); err != nil {
		return parquet2PCCompletedMarker{}, fmt.Errorf("failed to unmarshal parquet 2pc completed marker[%s]: %s", threadID, err)
	}
	if metadataState.ID == nil && metadataState.State == nil && len(metadataState.FullRefreshCommittedIDs) == 0 && metadataState.DedupInserts == nil {
		return marker, nil
	}
	marker.MetadataState = &metadataState
	return marker, nil
}

// promoteStaging moves committed staged files into the visible table path.
func (p *Parquet) promoteStaging(ctx context.Context, threadID string) error {
	if p.s3Client != nil {
		return p.promoteS3Staging(ctx, threadID)
	}
	return p.promoteLocalStaging(threadID)
}

// promoteLocalStaging renames local staged files into their final table paths.
func (p *Parquet) promoteLocalStaging(threadID string) error {
	stagingPath := p.localStagingPath(threadID)
	if _, err := os.Stat(stagingPath); os.IsNotExist(err) {
		return nil
	}

	return filepath.WalkDir(stagingPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(stagingPath, path)
		if err != nil {
			return err
		}
		if relPath == parquet2PCCompletedFile {
			return nil
		}

		finalPath := filepath.Join(p.config.Path, p.basePath, relPath)
		if err := os.MkdirAll(filepath.Dir(finalPath), os.ModePerm); err != nil {
			return err
		}
		return os.Rename(path, finalPath)
	})
}

// promoteS3Staging copies committed S3 staged objects into their final table keys.
func (p *Parquet) promoteS3Staging(ctx context.Context, threadID string) error {
	stagingPrefix := p.s3StagingPrefix(threadID)
	var pageErr error

	err := p.retryS3(ctx, func(ctx context.Context) error {
		pageErr = nil
		return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(stagingPrefix),
		}, func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, obj := range page.Contents {
				if obj.Key == nil {
					continue
				}
				relPath := strings.TrimPrefix(*obj.Key, stagingPrefix)
				if relPath == "" || relPath == parquet2PCCompletedFile {
					continue
				}
				finalKey := p.s3ObjectPath(filepath.Join(p.basePath, relPath))
				if err := p.copyS3Object(ctx, *obj.Key, finalKey); err != nil {
					pageErr = err
					return false
				}
				if err := p.deleteS3Object(ctx, *obj.Key); err != nil {
					logger.Warnf("Thread[%s]: failed to delete staged parquet object[%s]: %s", threadID, *obj.Key, err)
				}
			}
			return true
		})
	})
	if err != nil {
		return err
	}
	return pageErr
}

// copyS3Object copies one staged object into its final S3 key.
func (p *Parquet) copyS3Object(ctx context.Context, sourceKey, destinationKey string) error {
	return p.retryS3(ctx, func(ctx context.Context) error {
		_, err := p.s3Client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(p.config.Bucket),
			CopySource: aws.String(p.s3CopySource(sourceKey)),
			Key:        aws.String(destinationKey),
		})
		return err
	})
}

// cleanupCommittedStaging removes local staging data after committed promotion.
func (p *Parquet) cleanupCommittedStaging(threadID string) error {
	if p.s3Client != nil {
		// S3 has no empty directories; promotion deletes staged data objects and keeps _completed.json.
		return nil
	}
	return p.cleanupLocalCommittedStaging(threadID)
}

// cleanupLocalCommittedStaging keeps the completed marker and removes empty local partition dirs.
func (p *Parquet) cleanupLocalCommittedStaging(threadID string) error {
	stagingPath := p.localStagingPath(threadID)
	var dirs []string

	err := filepath.WalkDir(stagingPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != stagingPath {
			dirs = append(dirs, path)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for idx := len(dirs) - 1; idx >= 0; idx-- {
		dir := dirs[idx]
		if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// deleteStagingByName removes an uncommitted staging folder.
func (p *Parquet) deleteStagingByName(ctx context.Context, stagingDir string) error {
	if p.s3Client != nil {
		err := p.deleteS3Prefix(ctx, p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, stagingDir))+"/")
		if err != nil {
			return fmt.Errorf("failed to delete parquet 2pc staging dir[%s]: %s", stagingDir, err)
		}
		return nil
	}

	if err := os.RemoveAll(filepath.Join(p.local2PCPath(), stagingDir)); err != nil {
		return fmt.Errorf("failed to delete parquet 2pc staging dir[%s]: %s", stagingDir, err)
	}
	return nil
}

// deleteS3Prefix deletes all objects under an S3 prefix.
func (p *Parquet) deleteS3Prefix(ctx context.Context, prefix string) error {
	var pageErr error
	err := p.retryS3(ctx, func(ctx context.Context) error {
		pageErr = nil
		return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(prefix),
		}, func(page *s3.ListObjectsOutput, _ bool) bool {
			keys := make([]string, 0, len(page.Contents))
			for _, obj := range page.Contents {
				if obj.Key != nil {
					keys = append(keys, *obj.Key)
				}
			}
			if len(keys) == 0 {
				return true
			}
			concurrency := min(len(keys), 8)
			pageErr = utils.Concurrent(ctx, keys, concurrency, func(_ context.Context, key string, _ int) error {
				return p.deleteS3Object(ctx, key)
			})
			return pageErr == nil
		})
	})
	if err != nil {
		return err
	}
	return pageErr
}

// deleteS3Object deletes one S3 object.
func (p *Parquet) deleteS3Object(ctx context.Context, key string) error {
	return p.retryS3(ctx, func(ctx context.Context) error {
		_, err := p.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(key),
		})
		return err
	})
}

// write2PCObject writes a local or S3 object under the table 2PC prefix.
func (p *Parquet) write2PCObject(ctx context.Context, name string, data []byte) error {
	if p.s3Client != nil {
		return p.retryS3(ctx, func(ctx context.Context) error {
			_, err := p.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: aws.String(p.config.Bucket),
				Key:    aws.String(p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, name))),
				Body:   bytes.NewReader(data),
			})
			return err
		})
	}
	return writeLocalFile(filepath.Join(p.local2PCPath(), name), data)
}

// readS3Object reads an S3 object body into memory.
func (p *Parquet) readS3Object(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := p.retryS3(ctx, func(ctx context.Context) error {
		res, err := p.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return err
		}
		defer res.Body.Close()

		data, err = io.ReadAll(res.Body)
		return err
	})
	return data, err
}

// retryS3 retries S3 operations with the same rate-limit policy used by Parquet cleanup.
func (p *Parquet) retryS3(ctx context.Context, fn func(context.Context) error) error {
	return utils.RetryWithSkip(ctx, 3, time.Minute, isRateLimitError, fn)
}

// local2PCPath returns the local table 2PC directory.
func (p *Parquet) local2PCPath() string {
	return filepath.Join(p.config.Path, p.basePath, parquet2PCDir)
}

// localStagingPath returns a thread's local staging directory.
func (p *Parquet) localStagingPath(threadID string) string {
	return filepath.Join(p.local2PCPath(), p.stagingDirName(threadID))
}

// stagingDataDir returns the hidden staging directory corresponding to a final table directory.
func (p *Parquet) stagingDataDir(finalDir string) string {
	relPath, err := filepath.Rel(p.basePath, finalDir)
	if err != nil || relPath == "." {
		relPath = ""
	}
	return filepath.Join(p.basePath, parquet2PCDir, p.stagingDirName(p.options.ThreadID), relPath)
}

// stagingDataPath maps a final table file path to the current thread's staging path.
func (p *Parquet) stagingDataPath(finalPath string) string {
	return filepath.Join(p.stagingDataDir(filepath.Dir(finalPath)), filepath.Base(finalPath))
}

// s3StagingPrefix returns the S3 prefix for a thread's staging objects.
func (p *Parquet) s3StagingPrefix(threadID string) string {
	return p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, p.stagingDirName(threadID))) + "/"
}

// s3ObjectPath applies the configured prefix to a table-relative S3 key.
func (p *Parquet) s3ObjectPath(relativePath string) string {
	prefix := strings.Trim(p.config.Prefix, "/")
	if prefix == "" {
		return relativePath
	}
	return filepath.Join(prefix, relativePath)
}

// s3CopySource builds the URL-escaped CopySource value expected by S3.
func (p *Parquet) s3CopySource(key string) string {
	escapedKey := strings.ReplaceAll(url.PathEscape(key), "%2F", "/")
	return p.config.Bucket + "/" + escapedKey
}

// completedMarkerName returns the marker path relative to the table 2PC directory.
func (p *Parquet) completedMarkerName(threadID string) string {
	return filepath.Join(p.stagingDirName(threadID), parquet2PCCompletedFile)
}

// stagingDirName returns the escaped staging directory name for a thread.
func (p *Parquet) stagingDirName(threadID string) string {
	return p.encodedThreadID(threadID) + parquet2PCStagingSuffix
}

// encodedThreadID makes thread IDs safe for local and S3 path segments.
func (p *Parquet) encodedThreadID(threadID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(threadID))
}

// threadIDFromStagingDir decodes the original thread ID from a staging directory name.
func threadIDFromStagingDir(name string) (string, error) {
	encodedID := strings.TrimSuffix(name, parquet2PCStagingSuffix)
	data, err := base64.RawURLEncoding.DecodeString(encodedID)
	if err != nil {
		return "", fmt.Errorf("failed to decode parquet 2pc staging dir[%s]: %s", name, err)
	}
	return string(data), nil
}

// writeLocalFile writes a file through a temp file and local rename.
func writeLocalFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
