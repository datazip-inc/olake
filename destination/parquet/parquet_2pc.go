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
	parquet2PCDir              = "_olake_2pc"
	parquet2PCFinishFile       = "finish.json"
	parquet2PCMetadataFile     = "metadata.json"
	parquet2PCEmptyFinishState = "{}"
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
			// Without finish.json, the previous attempt did not reach the commit boundary.
			if err := p.deleteStagingPrefix(ctx, entry.prefix); err != nil {
				return err
			}
			continue
		}

		// A finished attempt is rolled forward before its staging objects are removed.
		if err := p.promoteS3Staging(ctx, entry.prefix); err != nil {
			return err
		}
		if err := p.commitBackfillMetadata(ctx, entry.threadID); err != nil {
			return err
		}
		if err := p.deleteStagingPrefix(ctx, entry.prefix); err != nil {
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

	finishKey := p.sharedFinishObjectKey()
	if !slices.Contains(keys, finishKey) {
		return p.deleteStagingPrefix(ctx, prefix)
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
	if err := p.commitStreamMetadata(ctx, *finishedState); err != nil {
		return err
	}
	return p.deleteStagingPrefix(ctx, prefix)
}

// streamFinishState serializes the abstract-provided state for finish.json and returns
// the same state in the typed form used to update metadata.json.
func streamFinishState(finalMetadataState any) ([]byte, types.MetadataState, error) {
	data, err := json.Marshal(finalMetadataState)
	if err != nil {
		return nil, types.MetadataState{}, fmt.Errorf("failed to marshal parquet 2pc finish state: %s", err)
	}
	state, err := parseFinishState(data)
	if err != nil {
		return nil, types.MetadataState{}, err
	}
	if state == nil {
		return nil, types.MetadataState{}, fmt.Errorf("cannot commit parquet CDC or incremental files without metadata state")
	}
	return data, *state, nil
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
	key := p.sharedFinishObjectKey()
	if p.options.Backfill {
		key = p.backfillFinishObjectKey(p.options.ThreadID)
	}
	return p.writeS3Object(ctx, key, data)
}

func (p *Parquet) commitBackfillMetadata(ctx context.Context, threadID string) error {
	state, err := p.readMetadata(ctx)
	if err != nil {
		return err
	}
	if state == nil {
		state = &types.MetadataState{}
	}

	if !slices.Contains(state.FullRefreshCommittedIDs, threadID) {
		state.FullRefreshCommittedIDs = append(state.FullRefreshCommittedIDs, threadID)
	}
	dedupInserts := true
	state.DedupInserts = &dedupInserts
	return p.writeMetadata(ctx, state)
}

func (p *Parquet) commitStreamMetadata(ctx context.Context, finishedState types.MetadataState) error {
	state, err := p.readMetadata(ctx)
	if err != nil {
		return err
	}
	if state == nil {
		state = &types.MetadataState{}
	}
	if err := mergeStreamMetadataState(state, &finishedState); err != nil {
		return err
	}
	return p.writeMetadata(ctx, state)
}

func (p *Parquet) writeMetadata(ctx context.Context, state *types.MetadataState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal parquet 2pc metadata: %s", err)
	}
	if err := p.writeS3Object(ctx, p.metadataObjectKey(), data); err != nil {
		return fmt.Errorf("failed to write parquet 2pc metadata: %s", err)
	}
	return nil
}

// mergeStreamMetadataState preserves checkpoints committed by parallel writers for the same stream.
func mergeStreamMetadataState(current, next *types.MetadataState) error {
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

	entries := make([]parquet2PCStagingEntry, 0)
	for _, key := range keys {
		relativePath := strings.TrimPrefix(key, rootPrefix)
		// For "<encoded-thread>/partition/data.parquet", parts[0] is the thread directory and parts[1] is the staged path.
		parts := strings.SplitN(relativePath, "/", 2)
		if len(parts) != 2 {
			continue
		}

		encodedThreadID, stagedPath := parts[0], parts[1]
		entryPrefix := rootPrefix + encodedThreadID + "/"
		if len(entries) == 0 || entries[len(entries)-1].prefix != entryPrefix {
			threadID, err := decodeThreadID(encodedThreadID)
			if err != nil {
				return nil, err
			}
			entries = append(entries, parquet2PCStagingEntry{
				prefix:   entryPrefix,
				threadID: threadID,
			})
		}
		if stagedPath == parquet2PCFinishFile {
			entries[len(entries)-1].finished = true
		}
	}
	return entries, nil
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

		finalKey := p.s3ObjectKey(path.Join(p.basePath, relativePath))
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
	return p.deleteStagingPrefix(ctx, p.currentStagingPrefix())
}

func (p *Parquet) deleteStagingPrefix(ctx context.Context, prefix string) error {
	keys, err := p.listS3Keys(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	finishKey := prefix + parquet2PCFinishFile
	dataKeys := make([]string, 0, len(keys))
	hasFinish := false
	for _, key := range keys {
		if key == finishKey {
			hasFinish = true
			continue
		}
		dataKeys = append(dataKeys, key)
	}
	if len(dataKeys) > 0 {
		if err := utils.Concurrent(ctx, dataKeys, min(len(dataKeys), 8), func(deleteCtx context.Context, key string, _ int) error {
			return p.deleteS3Object(deleteCtx, key)
		}); err != nil {
			return err
		}
	}
	if hasFinish {
		return p.deleteS3Object(ctx, finishKey)
	}
	return nil
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
	return p.s3ObjectKey(path.Join(p.basePath, parquet2PCDir)) + "/"
}

func (p *Parquet) backfillStagingPrefix(threadID string) string {
	return p.stagingRootPrefix() + encodeThreadID(threadID) + "/"
}

func (p *Parquet) currentStagingPrefix() string {
	if p.options.Backfill {
		return p.backfillStagingPrefix(p.options.ThreadID)
	}
	return p.stagingRootPrefix()
}

func (p *Parquet) stagingObjectKey(relativePath string) string {
	return p.currentStagingPrefix() + strings.TrimLeft(relativePath, "/")
}

func (p *Parquet) sharedFinishObjectKey() string {
	return p.stagingRootPrefix() + parquet2PCFinishFile
}

func (p *Parquet) backfillFinishObjectKey(threadID string) string {
	return p.backfillStagingPrefix(threadID) + parquet2PCFinishFile
}

func (p *Parquet) metadataObjectKey() string {
	return p.s3ObjectKey(path.Join(p.basePath, parquet2PCMetadataFile))
}

func (p *Parquet) s3ObjectKey(relativePath string) string {
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
