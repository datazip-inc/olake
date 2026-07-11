package parquet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

const (
	parquet2PCDir          = "_olake_2pc"
	parquet2PCFinishFile   = "finish.json"
	parquet2PCMetadataFile = "metadata.json"
)

type parquet2PCStagingEntry struct {
	stagingDir    string
	threadID      string
	finishedState *types.MetadataState
	finished      bool
}

func (p *Parquet) load2PCState(ctx context.Context, isFirstSetup bool) (*types.MetadataState, error) {
	if isFirstSetup && p.s3Client != nil {
		if err := os.RemoveAll(p.local2PCPath()); err != nil {
			return nil, fmt.Errorf("failed to cleanup local parquet 2pc staging: %s", err)
		}
	}

	entries, err := p.listStagingEntries(ctx)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.finished {
			if isFirstSetup {
				if err := p.deleteStaging(ctx, entry.threadID); err != nil {
					return nil, err
				}
			}
			continue
		}

		if err := p.promoteStaging(ctx, entry.threadID); err != nil {
			return nil, err
		}
		if _, err := p.commitMetadata(ctx, entry.threadID, entry.finishedState); err != nil {
			return nil, err
		}
		if err := p.deleteStaging(ctx, entry.threadID); err != nil {
			return nil, err
		}
	}

	return p.readMetadata(ctx)
}

func finishState(finalMetadataState any) ([]byte, *types.MetadataState, error) {
	if finalMetadataState == nil {
		return []byte("{}"), nil, nil
	}

	data, err := json.Marshal(finalMetadataState)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal parquet 2pc finish state: %s", err)
	}
	state, err := parseFinishState(data)
	if err != nil {
		return nil, nil, err
	}
	return data, state, nil
}

func parseFinishState(data []byte) (*types.MetadataState, error) {
	trimmedData := bytes.TrimSpace(data)
	if len(trimmedData) == 0 || bytes.Equal(trimmedData, []byte("{}")) || bytes.Equal(trimmedData, []byte("null")) {
		return nil, nil //nolint:nilnil // empty finish state denotes a full-refresh commit
	}

	var state types.MetadataState
	if err := json.Unmarshal(trimmedData, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal parquet 2pc finish state: %s", err)
	}
	return &state, nil
}

func (p *Parquet) writeFinish(ctx context.Context, data []byte) error {
	return p.writeObject(ctx, p.finishPath(p.options.ThreadID), data)
}

func (p *Parquet) commitMetadata(ctx context.Context, threadID string, finishedState *types.MetadataState) (*types.MetadataState, error) {
	state, err := p.readMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if state == nil {
		state = &types.MetadataState{}
	}

	if finishedState == nil {
		if !slices.Contains(state.FullRefreshCommittedIDs, threadID) {
			state.FullRefreshCommittedIDs = append(state.FullRefreshCommittedIDs, threadID)
		}
		dedupInserts := true
		state.DedupInserts = &dedupInserts
	} else {
		mergeMetadataState(state, finishedState)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal parquet 2pc metadata: %s", err)
	}
	if err := p.writeObject(ctx, p.metadataPath(), data); err != nil {
		return nil, fmt.Errorf("failed to write parquet 2pc metadata: %s", err)
	}
	return state, nil
}

func mergeMetadataState(current, next *types.MetadataState) {
	if next.ID != nil {
		current.ID = next.ID
	}
	if next.State != nil {
		current.State = next.State
	}
	for _, threadID := range next.FullRefreshCommittedIDs {
		if !slices.Contains(current.FullRefreshCommittedIDs, threadID) {
			current.FullRefreshCommittedIDs = append(current.FullRefreshCommittedIDs, threadID)
		}
	}
	if next.DedupInserts != nil {
		current.DedupInserts = next.DedupInserts
	}
}

func (p *Parquet) readMetadata(ctx context.Context) (*types.MetadataState, error) {
	data, err := p.readObject(ctx, p.metadataPath())
	if err != nil {
		if isObjectNotFound(err) {
			return nil, nil //nolint:nilnil // missing metadata denotes a fresh table
		}
		return nil, fmt.Errorf("failed to read parquet 2pc metadata: %s", err)
	}

	var state types.MetadataState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal parquet 2pc metadata: %s", err)
	}
	return &state, nil
}

func (p *Parquet) listStagingEntries(ctx context.Context) ([]parquet2PCStagingEntry, error) {
	if p.s3Client != nil {
		return p.listS3StagingEntries(ctx)
	}
	return p.listLocalStagingEntries()
}

func (p *Parquet) listLocalStagingEntries() ([]parquet2PCStagingEntry, error) {
	entries, err := os.ReadDir(p.local2PCPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	stagingEntries := make([]parquet2PCStagingEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		threadID, err := decodeThreadID(entry.Name())
		if err != nil {
			return nil, err
		}
		stagingEntry := parquet2PCStagingEntry{
			stagingDir: entry.Name(),
			threadID:   threadID,
		}
		data, err := os.ReadFile(filepath.Join(p.local2PCPath(), entry.Name(), parquet2PCFinishFile))
		if err != nil {
			if os.IsNotExist(err) {
				stagingEntries = append(stagingEntries, stagingEntry)
				continue
			}
			return nil, err
		}

		stagingEntry.finished = true
		stagingEntry.finishedState, err = parseFinishState(data)
		if err != nil {
			return nil, err
		}
		stagingEntries = append(stagingEntries, stagingEntry)
	}

	sort.Slice(stagingEntries, func(i, j int) bool {
		return stagingEntries[i].stagingDir < stagingEntries[j].stagingDir
	})
	return stagingEntries, nil
}

func (p *Parquet) listS3StagingEntries(ctx context.Context) ([]parquet2PCStagingEntry, error) {
	prefix := p.s3ObjectPath(filepath.Join(p.basePath, parquet2PCDir)) + "/"
	entries := make(map[string]*parquet2PCStagingEntry)
	var pageErr error

	err := p.retryS3(ctx, func(ctx context.Context) error {
		pageErr = nil
		entries = make(map[string]*parquet2PCStagingEntry)
		return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(prefix),
		}, func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, object := range page.Contents {
				if object.Key == nil {
					continue
				}
				relativePath := strings.TrimPrefix(*object.Key, prefix)
				parts := strings.SplitN(relativePath, "/", 2)
				if len(parts) != 2 {
					continue
				}

				entry, exists := entries[parts[0]]
				if !exists {
					threadID, err := decodeThreadID(parts[0])
					if err != nil {
						pageErr = err
						return false
					}
					entry = &parquet2PCStagingEntry{stagingDir: parts[0], threadID: threadID}
					entries[parts[0]] = entry
				}
				if parts[1] == parquet2PCFinishFile {
					entry.finished = true
				}
			}
			return true
		})
	})
	if err != nil {
		return nil, err
	}
	if pageErr != nil {
		return nil, pageErr
	}

	stagingEntries := make([]parquet2PCStagingEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.finished {
			data, err := p.readObject(ctx, p.finishPath(entry.threadID))
			if err != nil {
				return nil, err
			}
			entry.finishedState, err = parseFinishState(data)
			if err != nil {
				return nil, err
			}
		}
		stagingEntries = append(stagingEntries, *entry)
	}
	sort.Slice(stagingEntries, func(i, j int) bool {
		return stagingEntries[i].stagingDir < stagingEntries[j].stagingDir
	})
	return stagingEntries, nil
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

	return filepath.WalkDir(stagingPath, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		relativePath, err := filepath.Rel(stagingPath, path)
		if err != nil {
			return err
		}
		if relativePath == parquet2PCFinishFile {
			return nil
		}

		finalPath := filepath.Join(p.config.Path, p.basePath, relativePath)
		if err := os.MkdirAll(filepath.Dir(finalPath), os.ModePerm); err != nil {
			return err
		}
		return os.Rename(path, finalPath)
	})
}

func (p *Parquet) promoteS3Staging(ctx context.Context, threadID string) error {
	stagingPrefix := p.s3StagingPrefix(threadID)
	var pageErr error

	err := p.retryS3(ctx, func(ctx context.Context) error {
		pageErr = nil
		return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(stagingPrefix),
		}, func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, object := range page.Contents {
				if object.Key == nil {
					continue
				}
				relativePath := strings.TrimPrefix(*object.Key, stagingPrefix)
				if relativePath == "" || relativePath == parquet2PCFinishFile {
					continue
				}

				finalKey := p.s3ObjectPath(filepath.Join(p.basePath, relativePath))
				if err := p.copyS3Object(ctx, *object.Key, finalKey); err != nil {
					pageErr = err
					return false
				}
				if err := p.deleteS3Object(ctx, *object.Key); err != nil {
					pageErr = err
					return false
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

func (p *Parquet) deleteStaging(ctx context.Context, threadID string) error {
	if p.s3Client != nil {
		if err := p.deleteS3Prefix(ctx, p.s3StagingPrefix(threadID)); err != nil {
			return fmt.Errorf("failed to delete parquet 2pc staging for thread[%s]: %s", threadID, err)
		}
	}
	if err := os.RemoveAll(p.localStagingPath(threadID)); err != nil {
		return fmt.Errorf("failed to delete local parquet 2pc staging for thread[%s]: %s", threadID, err)
	}
	return nil
}

func (p *Parquet) deleteS3Prefix(ctx context.Context, prefix string) error {
	var pageErr error
	err := p.retryS3(ctx, func(ctx context.Context) error {
		pageErr = nil
		return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(prefix),
		}, func(page *s3.ListObjectsOutput, _ bool) bool {
			keys := make([]string, 0, len(page.Contents))
			for _, object := range page.Contents {
				if object.Key != nil {
					keys = append(keys, *object.Key)
				}
			}
			if len(keys) == 0 {
				return true
			}

			pageErr = utils.Concurrent(ctx, keys, min(len(keys), 8), func(_ context.Context, key string, _ int) error {
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

func (p *Parquet) deleteS3Object(ctx context.Context, key string) error {
	return p.retryS3(ctx, func(ctx context.Context) error {
		_, err := p.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(key),
		})
		return err
	})
}

func (p *Parquet) writeObject(ctx context.Context, relativePath string, data []byte) error {
	if p.s3Client != nil {
		return p.retryS3(ctx, func(ctx context.Context) error {
			_, err := p.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: aws.String(p.config.Bucket),
				Key:    aws.String(p.s3ObjectPath(relativePath)),
				Body:   bytes.NewReader(data),
			})
			return err
		})
	}
	return writeLocalFile(filepath.Join(p.config.Path, relativePath), data)
}

func (p *Parquet) readObject(ctx context.Context, relativePath string) ([]byte, error) {
	if p.s3Client == nil {
		return os.ReadFile(filepath.Join(p.config.Path, relativePath))
	}

	var data []byte
	err := p.retryS3(ctx, func(ctx context.Context) error {
		result, err := p.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(p.s3ObjectPath(relativePath)),
		})
		if err != nil {
			return err
		}
		defer result.Body.Close()

		data, err = io.ReadAll(result.Body)
		return err
	})
	return data, err
}

func isObjectNotFound(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	var awsErr awserr.Error
	return errors.As(err, &awsErr) && (awsErr.Code() == s3.ErrCodeNoSuchKey || awsErr.Code() == "NotFound")
}

func (p *Parquet) retryS3(ctx context.Context, fn func(context.Context) error) error {
	return utils.RetryWithSkip(ctx, 3, time.Minute, isRateLimitError, fn)
}

func (p *Parquet) local2PCPath() string {
	return filepath.Join(p.config.Path, p.basePath, parquet2PCDir)
}

func (p *Parquet) localStagingPath(threadID string) string {
	return filepath.Join(p.local2PCPath(), p.stagingDirName(threadID))
}

func (p *Parquet) stagingDataDir(finalDir string) string {
	relativePath, err := filepath.Rel(p.basePath, finalDir)
	if err != nil || relativePath == "." {
		relativePath = ""
	}
	return filepath.Join(p.basePath, parquet2PCDir, p.stagingDirName(p.options.ThreadID), relativePath)
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

func (p *Parquet) finishPath(threadID string) string {
	return filepath.Join(p.basePath, parquet2PCDir, p.stagingDirName(threadID), parquet2PCFinishFile)
}

func (p *Parquet) metadataPath() string {
	return filepath.Join(p.basePath, parquet2PCMetadataFile)
}

func (p *Parquet) stagingDirName(threadID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(threadID))
}

func decodeThreadID(name string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(name)
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
