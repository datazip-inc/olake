package driver

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/jsonschema"
	"github.com/datazip-inc/olake/jsonschema/schema"
	"github.com/datazip-inc/olake/logger"
	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/gobwas/glob"
	"github.com/piyushsingariya/drivers/s3/internal/reader"
	"github.com/piyushsingariya/relec/safego"
)

const patternSymbols = "*[]!{}"

type S3 struct {
	*base.Driver

	cursorField string
	session     *session.Session
	client      *s3.S3
	config      *Config
}

func (s *S3) Setup(config any, base *base.Driver) error {
	s.Driver = base

	cfg := Config{}
	err := utils.Unmarshal(config, &cfg)
	if err != nil {
		return err
	}

	err = cfg.Validate()
	if err != nil {
		return err
	}
	s.config = &cfg

	s.session, err = newSession(cfg.Credentials)
	if err != nil {
		return err
	}

	s.client = s3.New(s.session)
	s.cursorField = "last_modified_date"

	return nil
}

func (s *S3) Spec() (schema.JSONSchema, error) {
	return jsonschema.Reflect(Config{})
}

func (s *S3) Check() error {
	for stream, pattern := range s.config.Streams {
		err := s.iteration(types.ToPointer(int64(100)), pattern, 1, func(reader reader.Reader, file *s3.Object) (bool, error) {
			// break iteration after single item
			return false, nil
		})
		if err != nil {
			return fmt.Errorf("failed to check stream[%s] pattern[%s]: %s", stream, pattern, err)
		}
	}

	return nil
}

func (s *S3) Discover() ([]protocol.Stream, error) {
	streams := []protocol.Stream{}
	for stream, pattern := range s.config.Streams {
		var schema map[string]*types.Property
		var err error
		err = s.iteration(types.ToPointer(int64(100)), pattern, 1, func(reader reader.Reader, file *s3.Object) (bool, error) {
			schema, err = reader.GetSchema()
			return false, err
		})
		if err != nil {
			return nil, fmt.Errorf("failed to check stream[%s] pattern[%s]: %s", stream, pattern, err)
		}

		if schema == nil {
			return nil, fmt.Errorf("no schema found")
		}

		streams = append(streams, types.NewStream(stream, pattern).WithSyncMode(types.Incremental, types.FullRefresh).
			WithCursorField(s.cursorField).WithJSONSchema(types.Schema{
			Properties: schema,
		}))
	}

	return streams, nil
}

func (s *S3) Type() string {
	return "S3"
}

// NOTE: S3 read doesn't perform neccessary checks such as matching cursor field present in stream since
// it works only on single cursor field
func (s *S3) Read(stream protocol.Stream, channel chan<- types.Record) error {
	name, namespace := stream.Name(), stream.Namespace()
	// get pattern from stream name
	pattern := s.config.Streams[name]
	var localCursor *time.Time

	// if incremental check for state
	if stream.GetSyncMode() == types.Incremental {
		state := stream.InitialState()
		if state != nil {
			stateCursor, err := typeutils.ReformatDate(state)
			if err != nil {
				logger.Warnf("failed to parse state for stream %s[%s]", name, namespace)
			} else {
				localCursor = &stateCursor
			}
		} else {
			logger.Warnf("State not found for stream %s[%s]", name, namespace)
		}
	}

	err := s.iteration(types.ToPointer(stream.BatchSize()), pattern, s.config.PreLoadFactor, func(reader reader.Reader, file *s3.Object) (bool, error) {
		if localCursor != nil && file.LastModified.Before(*localCursor) {
			// continue iteration
			return true, nil
		}

		totalRecords := 0

		for reader.HasNext() {
			records, err := reader.Read()
			if err != nil {
				// discontinue iteration
				return false, fmt.Errorf("got error while reading records from %s[%s]: %s", name, namespace, err)
			}

			totalRecords += len(records)

			if len(records) == 0 {
				break
			}

			for _, record := range records {
				if !safego.Insert(channel, base.ReformatRecord(stream, record)) {
					// discontinue iteration since failed to insert records
					return false, nil
				}
			}
		}

		if localCursor == nil {
			localCursor = file.LastModified
		} else {
			localCursor = types.ToPointer((utils.MaxDate(*localCursor, *file.LastModified)))
		}

		logger.Infof("%d Records found in file %s", totalRecords, *file.Key)
		// go to next file
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("failed to read stream[%s] pattern[%s]: %s", name, pattern, err)
	}

	// update the state
	if stream.GetSyncMode() == types.Incremental {
		stream.SetState(localCursor)
	}

	return nil
}

func (s *S3) iteration(batchSize *int64, pattern string, preloadFactor int64, foreach func(reader reader.Reader, file *s3.Object) (bool, error)) error {
	re, err := glob.Compile(pattern)
	if err != nil {
		return fmt.Errorf("failed to complie file pattern please check: https://github.com/gobwas/glob#performance")
	}

	var continuationToken *string
	prefix := ""
	split := strings.Split(pattern, "/")
	for _, i := range split {
		if strings.ContainsAny(i, patternSymbols) {
			break
		}
		prefix = filepath.Join(prefix, i)
	}

	waitgroup := sync.WaitGroup{}
	consumer := make(chan struct {
		reader.Reader
		s3.Object
	}, preloadFactor)

	var breakIteration atomic.Bool
	breakIteration.Store(false)
	var consumerError error

	go func() {
		for file := range consumer {
			waitgroup.Add(1)

			// execute foreach
			next, err := foreach(file.Reader, &file.Object)
			if err != nil {
				consumerError = err
				safego.Close(consumer)
				waitgroup.Done()
				return
			}

			// break iteration
			if !next {
				safego.Close(consumer)
				waitgroup.Done()
				return
			}

			waitgroup.Done()
		}
	}()

	// List objects in the S3 bucket
s3Iteration:
	for {
		resp, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.Bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int64(10000000),
			ContinuationToken: continuationToken, // Initialize with nil
		})
		if err != nil {
			return fmt.Errorf("Error listing objects: %s", err)
		}

		// Iterate through the objects and process them
		for _, file := range resp.Contents {
			if re.Match(*file.Key) {
				re, err := reader.Init(s.client, s.config.Type, s.config.Bucket, *file.Key, batchSize)
				if err != nil {
					return fmt.Errorf("failed to initialize reader on file[%s]: %s", *file.Key, err)
				}

				if !safego.Insert(consumer, struct {
					reader.Reader
					s3.Object
				}{re, *file}) {
					break s3Iteration
				}
			}
		}

		// Check if there are more objects to retrieve
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break // Break the loop if there are no more objects
		}

		// Update the continuation token for the next iteration
		continuationToken := resp.NextContinuationToken
		if continuationToken == nil {
			break // Break the loop if the continuation token is nil (should not happen)
		}
	}

	waitgroup.Wait()

	return consumerError
}
