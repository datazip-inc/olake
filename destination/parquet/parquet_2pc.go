package parquet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	parquet2PCDir        = "_olake_2pc"
	parquet2PCStateFile  = "state.json"
	parquet2PCCommitsDir = "commits"
	parquet2PCPrepareDir = "preparing"
)

type parquet2PCMarker struct {
	ThreadID      string               `json:"thread_id"`
	Files         []string             `json:"files,omitempty"`
	MetadataState *types.MetadataState `json:"metadata_state,omitempty"`
	CommittedAt   time.Time            `json:"committed_at"`
}

type parquet2PCMarkerFile struct {
	parquet2PCMarker
	modTime time.Time
}

func (p *Parquet) load2PCState(ctx context.Context) (*types.MetadataState, error) {
	state, err := p.readStateFile(ctx)
	if err != nil {
		return nil, err
	}

	commits, err := p.list2PCMarkers(ctx, parquet2PCCommitsDir)
	if err != nil {
		return nil, err
	}

	committedIDs := make(map[string]bool, len(commits))
	var fullRefreshCommittedIDs []string
	var latestStateMarker *parquet2PCMarkerFile
	for _, commit := range commits {
		if commit.ThreadID == "" {
			continue
		}
		if !committedIDs[commit.ThreadID] {
			committedIDs[commit.ThreadID] = true
			if commit.MetadataState == nil {
				fullRefreshCommittedIDs = append(fullRefreshCommittedIDs, commit.ThreadID)
			}
		}
		if commit.MetadataState != nil && (latestStateMarker == nil || markerTime(commit).After(markerTime(*latestStateMarker))) {
			commitCopy := commit
			latestStateMarker = &commitCopy
		}
	}

	if state == nil && latestStateMarker != nil {
		state = latestStateMarker.MetadataState
	}
	if state == nil && len(fullRefreshCommittedIDs) == 0 {
		if err := p.cleanupPrepareMarkers(ctx, committedIDs, nil); err != nil {
			return nil, err
		}
		return nil, nil
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

	if err := p.cleanupPrepareMarkers(ctx, committedIDs, state); err != nil {
		return nil, err
	}
	return state, nil
}

func markerTime(marker parquet2PCMarkerFile) time.Time {
	if !marker.CommittedAt.IsZero() {
		return marker.CommittedAt
	}
	return marker.modTime
}

func (p *Parquet) metadataState(finalMetadataState any) (*types.MetadataState, error) {
	if finalMetadataState == nil {
		return nil, nil
	}

	var metadataState *types.MetadataState
	switch state := finalMetadataState.(type) {
	case *types.MetadataState:
		if state == nil {
			return nil, nil
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

func (p *Parquet) writePrepareMarker(ctx context.Context, files []parquetDataFile, metadataState *types.MetadataState) error {
	return p.write2PCMarker(ctx, parquet2PCPrepareDir, files, metadataState)
}

func (p *Parquet) writeCommitMarker(ctx context.Context, files []parquetDataFile, metadataState *types.MetadataState) error {
	return p.write2PCMarker(ctx, parquet2PCCommitsDir, files, metadataState)
}

func (p *Parquet) write2PCMarker(ctx context.Context, markerDir string, files []parquetDataFile, metadataState *types.MetadataState) error {
	marker := parquet2PCMarker{
		ThreadID:      p.options.ThreadID,
		Files:         dataFilePaths(files),
		MetadataState: metadataState,
		CommittedAt:   time.Now().UTC(),
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("failed to marshal parquet 2pc marker: %s", err)
	}
	return p.write2PCObject(ctx, filepath.Join(markerDir, p.markerFileName(p.options.ThreadID)), data)
}

func dataFilePaths(files []parquetDataFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return paths
}

func (p *Parquet) writeStateFile(ctx context.Context, metadataState *types.MetadataState) error {
	data, err := json.Marshal(metadataState)
	if err != nil {
		return fmt.Errorf("failed to marshal parquet 2pc state: %s", err)
	}
	return p.write2PCObject(ctx, parquet2PCStateFile, data)
}

func (p *Parquet) readStateFile(ctx context.Context) (*types.MetadataState, error) {
	data, err := p.read2PCObject(ctx, parquet2PCStateFile)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	var state types.MetadataState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal parquet 2pc state: %s", err)
	}
	return &state, nil
}

func (p *Parquet) list2PCMarkers(ctx context.Context, markerDir string) ([]parquet2PCMarkerFile, error) {
	if p.s3Client != nil {
		return p.listS3Markers(ctx, markerDir)
	}
	return p.listLocalMarkers(markerDir)
}

func (p *Parquet) listLocalMarkers(markerDir string) ([]parquet2PCMarkerFile, error) {
	dir := filepath.Join(p.local2PCPath(), markerDir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, nil
	}

	var markers []parquet2PCMarkerFile
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var marker parquet2PCMarker
		if err := json.Unmarshal(data, &marker); err != nil {
			return fmt.Errorf("failed to unmarshal parquet 2pc marker[%s]: %s", path, err)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		markers = append(markers, parquet2PCMarkerFile{
			parquet2PCMarker: marker,
			modTime:          info.ModTime(),
		})
		return nil
	})
	return markers, err
}

func (p *Parquet) listS3Markers(ctx context.Context, markerDir string) ([]parquet2PCMarkerFile, error) {
	prefix := p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, markerDir)) + "/"

	var markers []parquet2PCMarkerFile
	var pageErr error
	err := p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(p.config.Bucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListObjectsOutput, _ bool) bool {
		for _, obj := range page.Contents {
			if obj.Key == nil || !strings.HasSuffix(*obj.Key, ".json") {
				continue
			}
			data, err := p.readS3Object(ctx, *obj.Key)
			if err != nil {
				pageErr = err
				return false
			}
			var marker parquet2PCMarker
			if err := json.Unmarshal(data, &marker); err != nil {
				pageErr = fmt.Errorf("failed to unmarshal parquet 2pc marker[%s]: %s", *obj.Key, err)
				return false
			}
			modTime := time.Time{}
			if obj.LastModified != nil {
				modTime = *obj.LastModified
			}
			markers = append(markers, parquet2PCMarkerFile{
				parquet2PCMarker: marker,
				modTime:          modTime,
			})
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return markers, pageErr
}

func (p *Parquet) cleanupPrepareMarkers(ctx context.Context, committedIDs map[string]bool, metadataState *types.MetadataState) error {
	prepares, err := p.list2PCMarkers(ctx, parquet2PCPrepareDir)
	if err != nil {
		return err
	}

	for _, prepare := range prepares {
		if committedIDs[prepare.ThreadID] || metadataStateCommitted(metadataState, prepare.ThreadID) {
			if err := p.deletePrepareMarker(ctx, prepare.ThreadID); err != nil {
				logger.Warnf("Thread[%s]: failed to delete stale parquet 2pc prepare marker: %s", prepare.ThreadID, err)
			}
			continue
		}

		for _, file := range prepare.Files {
			if err := p.deleteDataFile(ctx, file); err != nil {
				return err
			}
		}
		if err := p.deletePrepareMarker(ctx, prepare.ThreadID); err != nil {
			return err
		}
	}
	return nil
}

func metadataStateCommitted(metadataState *types.MetadataState, threadID string) bool {
	if metadataState == nil || threadID == "" {
		return false
	}
	if fmt.Sprint(metadataState.ID) == threadID {
		return true
	}
	for _, id := range metadataState.FullRefreshCommittedIDs {
		if id == threadID {
			return true
		}
	}
	return false
}

func (p *Parquet) deleteDataFile(ctx context.Context, filePath string) error {
	if p.s3Client != nil {
		_, err := p.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(p.s3ObjectPath(filePath)),
		})
		return err
	}

	err := os.Remove(filepath.Join(p.config.Path, filePath))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (p *Parquet) deletePrepareMarker(ctx context.Context, threadID string) error {
	if threadID == "" {
		return nil
	}
	return p.delete2PCObject(ctx, filepath.Join(parquet2PCPrepareDir, p.markerFileName(threadID)))
}

func (p *Parquet) write2PCObject(ctx context.Context, name string, data []byte) error {
	if p.s3Client != nil {
		_, err := p.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, name))),
			Body:   strings.NewReader(string(data)),
		})
		return err
	}
	return writeLocalFile(filepath.Join(p.local2PCPath(), name), data)
}

func (p *Parquet) read2PCObject(ctx context.Context, name string) ([]byte, error) {
	if p.s3Client != nil {
		key := p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, name))
		data, err := p.readS3Object(ctx, key)
		if isS3NotFound(err) {
			return nil, nil
		}
		return data, err
	}

	data, err := os.ReadFile(filepath.Join(p.local2PCPath(), name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

func (p *Parquet) delete2PCObject(ctx context.Context, name string) error {
	if p.s3Client != nil {
		_, err := p.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir, name))),
		})
		return err
	}

	err := os.Remove(filepath.Join(p.local2PCPath(), name))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
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

func (p *Parquet) s3ObjectPath(relativePath string) string {
	prefix := strings.Trim(p.config.Prefix, "/")
	if prefix == "" {
		return relativePath
	}
	return filepath.Join(prefix, relativePath)
}

func (p *Parquet) markerFileName(threadID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(threadID)) + ".json"
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

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	var awsErr awserr.Error
	if !errors.As(err, &awsErr) {
		return false
	}
	return awsErr.Code() == s3.ErrCodeNoSuchKey || awsErr.Code() == "NotFound"
}
