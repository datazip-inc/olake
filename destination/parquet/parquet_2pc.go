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
	modTime       time.Time
}

func (p *Parquet) load2PCState(ctx context.Context) (*types.MetadataState, error) {
	markers, err := p.listCompletedMarkers(ctx)
	if err != nil {
		return nil, err
	}

	stagingDirs, err := p.listStagingDirs(ctx)
	if err != nil {
		return nil, err
	}

	completedStagingDirs := make(map[string]bool, len(markers))
	for _, marker := range markers {
		stagingDir := p.stagingDirName(marker.ThreadID)
		completedStagingDirs[stagingDir] = true
		if stagingDirs[stagingDir] {
			if err := p.promoteStaging(ctx, marker.ThreadID); err != nil {
				return nil, err
			}
			if err := p.cleanupCommittedStaging(marker.ThreadID); err != nil {
				logger.Warnf("Thread[%s]: failed to cleanup committed parquet 2pc staging dir: %s", marker.ThreadID, err)
			}
		}
	}

	for stagingDir := range stagingDirs {
		if !completedStagingDirs[stagingDir] {
			if err := p.deleteStagingByName(ctx, stagingDir); err != nil {
				return nil, err
			}
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
		if latestStateMarker == nil || marker.modTime.After(latestStateMarker.modTime) {
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

func (p *Parquet) listCompletedMarkers(ctx context.Context) ([]parquet2PCCompletedMarker, error) {
	if p.s3Client != nil {
		return p.listS3CompletedMarkers(ctx)
	}
	return p.listLocalCompletedMarkers()
}

func (p *Parquet) listLocalCompletedMarkers() ([]parquet2PCCompletedMarker, error) {
	entries, err := os.ReadDir(p.local2PCPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	markers := make([]parquet2PCCompletedMarker, 0)
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasSuffix(entry.Name(), parquet2PCStagingSuffix) {
			continue
		}
		marker, err := p.readLocalCompletedMarker(entry.Name())
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		markers = append(markers, marker)
	}
	return markers, nil
}

func (p *Parquet) listS3CompletedMarkers(ctx context.Context) ([]parquet2PCCompletedMarker, error) {
	prefix := p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir)) + "/"
	markers := make([]parquet2PCCompletedMarker, 0)
	var pageErr error

	err := p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(p.config.Bucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListObjectsOutput, _ bool) bool {
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			relPath := strings.TrimPrefix(*obj.Key, prefix)
			parts := strings.Split(relPath, "/")
			if len(parts) != 2 || !strings.HasSuffix(parts[0], parquet2PCStagingSuffix) || parts[1] != parquet2PCCompletedFile {
				continue
			}
			marker, err := p.readS3CompletedMarker(ctx, parts[0], *obj.Key, obj.LastModified)
			if err != nil {
				pageErr = err
				return false
			}
			markers = append(markers, marker)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return markers, pageErr
}

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

func (p *Parquet) readS3CompletedMarker(ctx context.Context, stagingDir, key string, lastModified *time.Time) (parquet2PCCompletedMarker, error) {
	threadID, err := threadIDFromStagingDir(stagingDir)
	if err != nil {
		return parquet2PCCompletedMarker{}, err
	}

	data, err := p.readS3Object(ctx, key)
	if err != nil {
		return parquet2PCCompletedMarker{}, err
	}

	modTime := time.Time{}
	if lastModified != nil {
		modTime = *lastModified
	}
	return p.parseCompletedMarker(threadID, data, modTime)
}

func (p *Parquet) parseCompletedMarker(threadID string, data []byte, modTime time.Time) (parquet2PCCompletedMarker, error) {
	marker := parquet2PCCompletedMarker{
		ThreadID: threadID,
		modTime:  modTime,
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

func (p *Parquet) promoteStaging(ctx context.Context, threadID string) error {
	if p.s3Client != nil {
		return p.promoteS3Staging(ctx, threadID)
	}
	return p.promoteLocalStaging(threadID)
}

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

func (p *Parquet) promoteS3Staging(ctx context.Context, threadID string) error {
	stagingPrefix := p.s3StagingPrefix(threadID)
	var pageErr error

	err := p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
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
	if err != nil {
		return err
	}
	return pageErr
}

func (p *Parquet) copyS3Object(ctx context.Context, sourceKey, destinationKey string) error {
	_, err := p.s3Client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(p.config.Bucket),
		CopySource: aws.String(p.s3CopySource(sourceKey)),
		Key:        aws.String(destinationKey),
	})
	return err
}

func (p *Parquet) cleanupCommittedStaging(threadID string) error {
	if p.s3Client != nil {
		return nil
	}
	return p.cleanupLocalCommittedStaging(threadID)
}

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
		if err := os.Remove(dir); err != nil && !os.IsNotExist(err) && !os.IsExist(err) {
			return err
		}
	}
	return nil
}

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

func (p *Parquet) listStagingDirs(ctx context.Context) (map[string]bool, error) {
	if p.s3Client != nil {
		return p.listS3StagingDirs(ctx)
	}
	return p.listLocalStagingDirs()
}

func (p *Parquet) listLocalStagingDirs() (map[string]bool, error) {
	entries, err := os.ReadDir(p.local2PCPath())
	if os.IsNotExist(err) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, err
	}

	stagingDirs := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() && strings.HasSuffix(entry.Name(), parquet2PCStagingSuffix) {
			stagingDirs[entry.Name()] = true
		}
	}
	return stagingDirs, nil
}

func (p *Parquet) listS3StagingDirs(ctx context.Context) (map[string]bool, error) {
	prefix := p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir)) + "/"
	stagingDirs := make(map[string]bool)
	var pageErr error

	err := p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(p.config.Bucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListObjectsOutput, _ bool) bool {
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			relPath := strings.TrimPrefix(*obj.Key, prefix)
			parts := strings.SplitN(relPath, "/", 2)
			if len(parts) > 0 && strings.HasSuffix(parts[0], parquet2PCStagingSuffix) {
				stagingDirs[parts[0]] = true
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return stagingDirs, pageErr
}

func (p *Parquet) deleteS3Prefix(ctx context.Context, prefix string) error {
	var pageErr error
	err := p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
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
	if err != nil {
		return err
	}
	return pageErr
}

func (p *Parquet) deleteS3Object(ctx context.Context, key string) error {
	_, err := p.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(p.config.Bucket),
		Key:    aws.String(key),
	})
	return err
}

func (p *Parquet) write2PCObject(ctx context.Context, name string, data []byte) error {
	if p.s3Client != nil {
		_, err := p.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, name))),
			Body:   bytes.NewReader(data),
		})
		return err
	}
	return writeLocalFile(filepath.Join(p.local2PCPath(), name), data)
}

func (p *Parquet) readS3Object(ctx context.Context, key string) ([]byte, error) {
	res, err := p.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}

func (p *Parquet) local2PCPath() string {
	return filepath.Join(p.config.Path, p.basePath, parquet2PCDir)
}

func (p *Parquet) localStagingPath(threadID string) string {
	return filepath.Join(p.local2PCPath(), p.stagingDirName(threadID))
}

func (p *Parquet) stagingDataDir(basePath string) string {
	relPath, err := filepath.Rel(p.basePath, basePath)
	if err != nil || relPath == "." {
		relPath = ""
	}
	return filepath.Join(p.basePath, parquet2PCDir, p.stagingDirName(p.options.ThreadID), relPath)
}

func (p *Parquet) stagingDataPath(finalPath string) string {
	return filepath.Join(p.stagingDataDir(filepath.Dir(finalPath)), filepath.Base(finalPath))
}

func (p *Parquet) s3StagingPrefix(threadID string) string {
	return p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, p.stagingDirName(threadID))) + "/"
}

func (p *Parquet) s3ObjectPath(relativePath string) string {
	prefix := strings.Trim(p.config.Prefix, "/")
	if prefix == "" {
		return relativePath
	}
	return filepath.Join(prefix, relativePath)
}

func (p *Parquet) s3CopySource(key string) string {
	escapedKey := strings.ReplaceAll(url.PathEscape(key), "%2F", "/")
	return p.config.Bucket + "/" + escapedKey
}

func (p *Parquet) completedMarkerName(threadID string) string {
	return filepath.Join(p.stagingDirName(threadID), parquet2PCCompletedFile)
}

func (p *Parquet) stagingDirName(threadID string) string {
	return p.encodedThreadID(threadID) + parquet2PCStagingSuffix
}

func (p *Parquet) encodedThreadID(threadID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(threadID))
}

func threadIDFromStagingDir(name string) (string, error) {
	encodedID := strings.TrimSuffix(name, parquet2PCStagingSuffix)
	data, err := base64.RawURLEncoding.DecodeString(encodedID)
	if err != nil {
		return "", fmt.Errorf("failed to decode parquet 2pc staging dir[%s]: %s", name, err)
	}
	return string(data), nil
}

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
