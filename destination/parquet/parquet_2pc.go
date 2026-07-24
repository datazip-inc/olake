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
	"path"
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
	prefix   string
	threadID string
	finished bool
}

// load2PCState rolls staged S3 commits forward before returning durable table metadata.
func (p *Parquet) load2PCState(ctx context.Context) (*types.MetadataState, error) {
	if err := p.recoverStaging(ctx); err != nil {
		return nil, err
	}
	return p.readMetadata(ctx)
}

// recoverStaging resolves the layout used by the current sync phase.
func (p *Parquet) recoverStaging(ctx context.Context) error {
	if p.options.Backfill {
		return p.recoverBackfillStaging(ctx)
	}
	return p.recoverSharedStaging(ctx)
}

// recoverBackfillStaging resolves each full-refresh chunk independently.
func (p *Parquet) recoverBackfillStaging(ctx context.Context) error {
	entries, err := p.listBackfillStagingEntries(ctx)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.finished {
			if err := p.deleteS3Prefix(ctx, entry.prefix); err != nil {
				return err
			}
			continue
		}

		data, err := p.readS3Object(ctx, entry.prefix+parquet2PCFinishFile)
		if err != nil {
			return err
		}
		finishedState, err := parseFinishState(data)
		if err != nil {
			return err
		}
		if finishedState != nil {
			return fmt.Errorf("parquet 2pc metadata state is not supported in full-refresh staging")
		}
		if err := p.promoteS3Staging(ctx, entry.prefix); err != nil {
			return err
		}
		if err := p.commitMetadata(ctx, entry.threadID, finishedState); err != nil {
			return err
		}
		if err := p.deleteS3Prefix(ctx, entry.prefix); err != nil {
			return err
		}
	}
	return nil
}

// recoverSharedStaging resolves the single CDC/incremental staging area.
func (p *Parquet) recoverSharedStaging(ctx context.Context) error {
	prefix := p.stagingRootPrefix()
	keys, err := p.listS3Keys(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	finishKey := prefix + parquet2PCFinishFile
	if !slices.Contains(keys, finishKey) {
		return p.deleteS3Prefix(ctx, prefix)
	}

	data, err := p.readS3Object(ctx, finishKey)
	if err != nil {
		return err
	}
	finishedState, err := parseFinishState(data)
	if err != nil {
		return err
	}
	if finishedState == nil {
		return fmt.Errorf("parquet 2pc metadata state is missing from shared staging")
	}
	if err := p.promoteS3Staging(ctx, prefix); err != nil {
		return err
	}
	if err := p.commitMetadata(ctx, p.options.ThreadID, finishedState); err != nil {
		return err
	}
	return p.deleteS3Prefix(ctx, prefix)
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
	return p.writeS3Object(ctx, p.currentFinishObjectKey(), data)
}

// commitMetadata records promoted progress in the table-level metadata file.
func (p *Parquet) commitMetadata(ctx context.Context, threadID string, finishedState *types.MetadataState) error {
	state, err := p.readMetadata(ctx)
	if err != nil {
		return err
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
	} else if err := mergeMetadataState(state, finishedState); err != nil {
		return err
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal parquet 2pc metadata: %s", err)
	}
	if err := p.writeS3Object(ctx, p.metadataObjectKey(), data); err != nil {
		return fmt.Errorf("failed to write parquet 2pc metadata: %s", err)
	}
	return nil
}

// mergeMetadataState preserves fields committed by parallel writers for the same stream.
func mergeMetadataState(current, next *types.MetadataState) error {
	if next.ID != nil {
		current.ID = next.ID
	}
	if next.State != nil {
		state, err := mergeState(current.State, next.State)
		if err != nil {
			return err
		}
		current.State = state
	}
	for _, threadID := range next.FullRefreshCommittedIDs {
		if !slices.Contains(current.FullRefreshCommittedIDs, threadID) {
			current.FullRefreshCommittedIDs = append(current.FullRefreshCommittedIDs, threadID)
		}
	}
	if next.DedupInserts != nil {
		current.DedupInserts = next.DedupInserts
	}
	return nil
}

func mergeState(current, next any) (any, error) {
	currentState, currentOK := jsonObject(current)
	nextState, nextOK := jsonObject(next)
	if !currentOK || !nextOK {
		return next, nil
	}

	for key, value := range nextState {
		currentState[key] = value
	}
	data, err := json.Marshal(currentState)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged parquet 2pc state: %s", err)
	}
	return string(data), nil
}

func jsonObject(value any) (map[string]any, bool) {
	state, ok := value.(string)
	if !ok {
		return nil, false
	}

	var result map[string]any
	decoder := json.NewDecoder(strings.NewReader(state))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, false
	}
	return result, true
}

// readMetadata returns the latest durable destination checkpoint.
func (p *Parquet) readMetadata(ctx context.Context) (*types.MetadataState, error) {
	data, err := p.readS3Object(ctx, p.metadataObjectKey())
	if err != nil {
		if isS3ObjectNotFound(err) {
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

// listBackfillStagingEntries groups S3 objects by full-refresh thread.
func (p *Parquet) listBackfillStagingEntries(ctx context.Context) ([]parquet2PCStagingEntry, error) {
	rootPrefix := p.stagingRootPrefix()
	keys, err := p.listS3Keys(ctx, rootPrefix)
	if err != nil {
		return nil, err
	}

	entries := make(map[string]*parquet2PCStagingEntry)
	for _, key := range keys {
		relativePath := strings.TrimPrefix(key, rootPrefix)
		parts := strings.SplitN(relativePath, "/", 2)
		if len(parts) != 2 {
			continue
		}

		entry, exists := entries[parts[0]]
		if !exists {
			threadID, err := decodeThreadID(parts[0])
			if err != nil {
				return nil, err
			}
			entry = &parquet2PCStagingEntry{
				prefix:   rootPrefix + parts[0] + "/",
				threadID: threadID,
			}
			entries[parts[0]] = entry
		}
		if parts[1] == parquet2PCFinishFile {
			entry.finished = true
		}
	}

	stagingEntries := make([]parquet2PCStagingEntry, 0, len(entries))
	for _, entry := range entries {
		stagingEntries = append(stagingEntries, *entry)
	}
	sort.Slice(stagingEntries, func(i, j int) bool {
		return stagingEntries[i].prefix < stagingEntries[j].prefix
	})
	return stagingEntries, nil
}

func (p *Parquet) promoteStaging(ctx context.Context) error {
	return p.promoteS3Staging(ctx, p.currentStagingPrefix())
}

// promoteS3Staging copies staged data into the visible table path.
func (p *Parquet) promoteS3Staging(ctx context.Context, stagingPrefix string) error {
	keys, err := p.listS3Keys(ctx, stagingPrefix)
	if err != nil {
		return err
	}

	for _, key := range keys {
		relativePath := strings.TrimPrefix(key, stagingPrefix)
		if relativePath == "" || relativePath == parquet2PCFinishFile {
			continue
		}

		finalKey := p.s3ObjectPath(path.Join(p.basePath, relativePath))
		if err := p.copyS3Object(ctx, key, finalKey); err != nil {
			return err
		}
		if err := p.deleteS3Object(ctx, key); err != nil {
			return err
		}
	}
	return nil
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

func (p *Parquet) deleteStaging(ctx context.Context) error {
	return p.deleteS3Prefix(ctx, p.currentStagingPrefix())
}

func (p *Parquet) deleteS3Prefix(ctx context.Context, prefix string) error {
	keys, err := p.listS3Keys(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return utils.Concurrent(ctx, keys, min(len(keys), 8), func(deleteCtx context.Context, key string, _ int) error {
		return p.deleteS3Object(deleteCtx, key)
	})
}

func (p *Parquet) listS3Keys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	err := p.retryS3(ctx, func(ctx context.Context) error {
		keys = keys[:0]
		return p.s3Client.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(p.config.Bucket),
			Prefix: aws.String(prefix),
		}, func(page *s3.ListObjectsOutput, _ bool) bool {
			for _, object := range page.Contents {
				if object.Key != nil {
					keys = append(keys, *object.Key)
				}
			}
			return true
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
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

func (p *Parquet) writeS3Object(ctx context.Context, key string, data []byte) error {
	return p.retryS3(ctx, func(ctx context.Context) error {
		_, err := p.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		return err
	})
}

func (p *Parquet) readS3Object(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := p.retryS3(ctx, func(ctx context.Context) error {
		result, err := p.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(p.config.Bucket),
			Key:    aws.String(key),
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

func isS3ObjectNotFound(err error) bool {
	var awsErr awserr.Error
	return errors.As(err, &awsErr) && (awsErr.Code() == s3.ErrCodeNoSuchKey || awsErr.Code() == "NotFound")
}

func (p *Parquet) retryS3(ctx context.Context, fn func(context.Context) error) error {
	return utils.RetryWithSkip(ctx, 3, time.Minute, isRateLimitError, fn)
}

func (p *Parquet) stagingRootPrefix() string {
	return p.s3ObjectPath(path.Join(p.basePath, parquet2PCDir)) + "/"
}

func (p *Parquet) currentStagingPrefix() string {
	if p.options.Backfill {
		return p.stagingRootPrefix() + encodeThreadID(p.options.ThreadID) + "/"
	}
	return p.stagingRootPrefix()
}

func (p *Parquet) stagingObjectKey(relativePath string) string {
	return p.currentStagingPrefix() + strings.TrimLeft(relativePath, "/")
}

func (p *Parquet) currentFinishObjectKey() string {
	return p.currentStagingPrefix() + parquet2PCFinishFile
}

func (p *Parquet) metadataObjectKey() string {
	return p.s3ObjectPath(path.Join(p.basePath, parquet2PCMetadataFile))
}

func (p *Parquet) s3ObjectPath(relativePath string) string {
	prefix := strings.Trim(p.config.Prefix, "/")
	if prefix == "" {
		return relativePath
	}
	return path.Join(prefix, relativePath)
}

func (p *Parquet) s3CopySource(key string) string {
	escapedKey := strings.ReplaceAll(url.PathEscape(key), "%2F", "/")
	return p.config.Bucket + "/" + escapedKey
}

func encodeThreadID(threadID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(threadID))
}

func decodeThreadID(name string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(name)
	if err != nil {
		return "", fmt.Errorf("failed to decode parquet 2pc staging dir[%s]: %s", name, err)
	}
	return string(data), nil
}
