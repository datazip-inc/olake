// Package parquetarrow implements arrowdst.DestinationAdapter for the
// arrow-native Parquet write path.
//
// Layering:
//
//	destination/arrow              — destination-neutral arrow library
//	                                 (BuildArrowRecord, RollingArrowWriter,
//	                                 schema mapping, FlattenAndDetect)
//	destination/arrow/parquet      — THIS PACKAGE: arrow Parquet adapter
//	                                 + its own Config (declared in config.go)
//	destination/legacy/parquet     — legacy parquet-go row writer
//
// Parquet has no Java side, so this adapter is purely Go.
package parquetarrow

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/datazip-inc/olake/constants"
	arrowdst "github.com/datazip-inc/olake/destination/arrow"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func init() {
	arrowdst.RegisteredAdapters[string(types.Parquet)] = func() arrowdst.DestinationAdapter {
		return &Adapter{}
	}
}

// Adapter implements arrowdst.DestinationAdapter for Parquet. It owns the
// per-flavor Config struct defined in config.go (same package).
type Adapter struct {
	cfg         *Config
	stream      types.StreamInterface
	upsertMode  bool
	basePath    string
	s3Uploader  *s3manager.Uploader
	arrowSchema *arrowlib.Schema
	// rollers is keyed by the partition path (relative to cfg.Path).
	rollers map[string]*arrowdst.RollingArrowWriter
}

// GetConfigRef returns a pointer to the shared parquet Config so the
// arrow Pool can unmarshal the raw writer config into it.
func (a *Adapter) GetConfigRef() any {
	if a.cfg == nil {
		a.cfg = &Config{}
	}
	return a.cfg
}

// Setup validates config, wires S3, and bootstraps the initial schema.
func (a *Adapter) Setup(_ context.Context, stream types.StreamInterface, upsertMode bool,
	incoming arrowdst.OLakeSchema,
) (arrowdst.Setup, error) {
	if a.cfg == nil {
		return arrowdst.Setup{}, fmt.Errorf("parquetarrowadapter: config not injected before Setup")
	}
	if err := a.cfg.Validate(); err != nil {
		return arrowdst.Setup{}, fmt.Errorf("parquetarrowadapter: config validation: %s", err)
	}

	a.stream = stream
	a.upsertMode = upsertMode
	a.basePath = filepath.Join(stream.GetDestinationDatabase(nil), stream.GetDestinationTable())
	a.rollers = make(map[string]*arrowdst.RollingArrowWriter)

	if a.cfg.Path == "" {
		a.cfg.Path = os.TempDir()
	}
	if err := a.initS3(); err != nil {
		return arrowdst.Setup{}, fmt.Errorf("parquetarrowadapter: init s3: %s", err)
	}

	schema := incoming
	if schema == nil {
		schema = make(arrowdst.OLakeSchema)
		for _, col := range stream.Schema().ColumnNames() {
			if dt, err := stream.Schema().GetType(col); err == nil {
				schema[col] = dt
			}
		}
	}

	return arrowdst.Setup{
		Schema: schema,
		Shape:  arrowdst.SchemaShape{}, // no field IDs, no identifier field for plain parquet
		State:  nil,                    // no 2PC for plain parquet
	}, nil
}

// WriteBatch writes a batch of records grouped by partition path. Records are
// normalised (i→c) before building the Arrow record since plain Parquet has no
// equality-delete concept.
func (a *Adapter) WriteBatch(ctx context.Context, records []types.RawRecord, arrowSchema *arrowlib.Schema) error {
	a.arrowSchema = arrowSchema

	groups := map[string][]types.RawRecord{}
	for _, rec := range records {
		arrowdst.NormaliseOpType(&rec) // i → c, no dedup involved
		olakeTs, _ := rec.OlakeColumns[constants.OlakeTimestamp].(time.Time)
		pPath := a.getPartitionedFilePath(rec.Data, olakeTs)
		groups[pPath] = append(groups[pPath], rec)
	}

	for pPath, group := range groups {
		roller, err := a.getOrCreateRoller(ctx, pPath)
		if err != nil {
			return err
		}
		arrowRec, err := arrowdst.BuildArrowRecord(group, arrowSchema, nil)
		if err != nil {
			return fmt.Errorf("build arrow record for partition %q: %s", pPath, err)
		}
		if err := roller.WriteRecord(ctx, arrowRec); err != nil {
			arrowRec.Release()
			return fmt.Errorf("write record to partition %q: %s", pPath, err)
		}
		arrowRec.Release()
	}
	return nil
}

// OnSchemaEvolved closes all open rollers so the next WriteBatch reopens them
// with the new arrow.Schema. Plain Parquet has no catalog to notify.
func (a *Adapter) OnSchemaEvolved(ctx context.Context, _ arrowdst.OLakeSchema) (arrowdst.SchemaShape, error) {
	for k, r := range a.rollers {
		if err := r.Close(ctx); err != nil {
			logger.Warnf("parquetarrowadapter: close roller %q on schema evolution: %s", k, err)
		}
	}
	a.rollers = make(map[string]*arrowdst.RollingArrowWriter)
	return arrowdst.SchemaShape{}, nil
}

// Close flushes all rollers and, on context cancellation, cleans up local
// temp files (matches the plain-parquet closeOnError behaviour).
func (a *Adapter) Close(ctx context.Context, _ any) error {
	var firstErr error
	for k, r := range a.rollers {
		if err := r.Close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close roller %q: %s", k, err)
		}
	}
	a.rollers = nil
	if ctx.Err() != nil {
		a.cleanupLocalTemp()
	}
	return firstErr
}

// ---------------------------------------------------------------------------
// Internal helpers — duplicated from destination/legacy/parquet/parquet.go on purpose so the
// two write paths can evolve independently without a shared-package dependency.
// ---------------------------------------------------------------------------

func (a *Adapter) getOrCreateRoller(ctx context.Context, partitionPath string) (*arrowdst.RollingArrowWriter, error) {
	if r, ok := a.rollers[partitionPath]; ok {
		return r, nil
	}

	cb := arrowdst.RollingCallbacks{
		Allocate: func(_ context.Context, _ string) (string, error) {
			localDir := filepath.Join(a.cfg.Path, partitionPath)
			if err := os.MkdirAll(localDir, 0o755); err != nil {
				return "", fmt.Errorf("mkdir %s: %s", localDir, err)
			}
			return filepath.Join(localDir, utils.TimestampedFileName(constants.ParquetFileExt)), nil
		},
		Persist: func(ctx context.Context, path, _ string, data []byte, _, _ int64) error {
			if err := os.WriteFile(path, data, 0o644); err != nil {
				return fmt.Errorf("write %s: %s", path, err)
			}
			if a.s3Uploader != nil {
				rel := strings.TrimPrefix(path, a.cfg.Path+string(filepath.Separator))
				s3Key := strings.TrimLeft(filepath.Join(a.cfg.Prefix, rel), string(filepath.Separator))
				if err := a.uploadToS3(ctx, s3Key, data); err != nil {
					return err
				}
				_ = os.Remove(path)
			}
			return nil
		},
	}

	const target512MiB = int64(512 * 1024 * 1024)
	roller, err := arrowdst.NewRollingArrowWriter(ctx, a.arrowSchema, "data", target512MiB, nil, cb)
	if err != nil {
		return nil, fmt.Errorf("new rolling writer for %q: %s", partitionPath, err)
	}
	a.rollers[partitionPath] = roller
	return roller, nil
}

func (a *Adapter) initS3() error {
	if a.cfg.Bucket == "" || a.cfg.Region == "" {
		return nil
	}
	awsCfg := aws.Config{Region: aws.String(a.cfg.Region)}
	if a.cfg.S3Endpoint != "" {
		awsCfg.Endpoint = aws.String(a.cfg.S3Endpoint)
		awsCfg.S3ForcePathStyle = aws.Bool(true)
	}
	if a.cfg.AccessKey != "" && a.cfg.SecretKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentials(a.cfg.AccessKey, a.cfg.SecretKey, "")
	}
	sess, err := session.NewSession(&awsCfg)
	if err != nil {
		return fmt.Errorf("aws session: %s", err)
	}
	a.s3Uploader = s3manager.NewUploader(sess)
	return nil
}

func (a *Adapter) uploadToS3(ctx context.Context, s3Key string, data []byte) error {
	_, err := a.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(a.cfg.Bucket),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 upload %s: %s", s3Key, err)
	}
	return nil
}

func (a *Adapter) getPartitionedFilePath(values map[string]any, olakeTimestamp time.Time) string {
	pattern := a.stream.Self().StreamMetadata.PartitionRegex
	if pattern == "" {
		return a.basePath
	}

	patternRegex := regexp.MustCompile(constants.PartitionRegexParquet)
	result := patternRegex.ReplaceAllStringFunc(pattern, func(match string) string {
		trimmed := strings.Trim(match, "{}")
		parts := strings.Split(trimmed, ",")
		if len(parts) < 3 {
			return ""
		}
		colName := strings.TrimSpace(strings.Trim(parts[0], `'`))
		defaultValue := strings.TrimSpace(strings.Trim(parts[1], `'`))
		granularity := strings.TrimSpace(strings.Trim(parts[2], `'`))
		if defaultValue == "" {
			defaultValue = fmt.Sprintf("default_%s", colName)
		}

		applyGranularity := func(v any) string {
			if granularity != "" {
				if ts, err := typeutils.ReformatValue(types.Timestamp, v); err == nil {
					if t, ok := ts.(time.Time); ok {
						switch granularity {
						case "HH":
							return fmt.Sprintf("%02d", t.UTC().Hour())
						case "DD":
							return fmt.Sprintf("%02d", t.UTC().Day())
						case "WW":
							_, week := t.UTC().ISOWeek()
							return fmt.Sprintf("%02d", week)
						case "MM":
							return fmt.Sprintf("%02d", int(t.UTC().Month()))
						case "YYYY":
							return fmt.Sprintf("%d", t.UTC().Year())
						}
					}
				}
			}
			return fmt.Sprintf("%v", v)
		}

		if colName == "now()" {
			return applyGranularity(olakeTimestamp)
		}
		if v, ok := values[colName]; ok && v != nil {
			return applyGranularity(v)
		}
		return defaultValue
	})

	if result == "" {
		return a.basePath
	}
	return filepath.Join(a.basePath, strings.TrimSuffix(result, "/"))
}

func (a *Adapter) cleanupLocalTemp() {
	if a.cfg == nil || a.cfg.Path == "" {
		return
	}
	basePath := filepath.Join(a.cfg.Path, a.basePath)
	if err := os.RemoveAll(basePath); err != nil {
		logger.Warnf("parquetarrowadapter: cleanupLocalTemp %s: %s", basePath, err)
	}
}
