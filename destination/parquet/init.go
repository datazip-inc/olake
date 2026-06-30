package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	s3sink "github.com/datazip-inc/olake/writers/sink/s3"
)

// register empty Parquet instance
func init() {
	destination.RegisteredWriters[types.Parquet] = func() destination.Writer {
		return new(Parquet)
	}
}

// Setup is used as a constructor for the registered empty Parquet instance
func (p *Parquet) Setup(_ context.Context, stream types.StreamInterface, schema any, options *destination.Options) (any, *types.MetadataState, error) {
	p.options = options
	p.stream = stream
	p.basePath = filepath.Join(p.stream.GetDestinationDatabase(nil), p.stream.GetDestinationTable())
	p.schema = make(typeutils.Fields)

	// for s3 p.config.path may not be provided
	if p.config.Path == "" {
		p.config.Path = os.TempDir()
	}

	// Stand up the control-plane S3 sink (Check/DropStreams use it, and it flags S3
	// mode for newSink) when object-storage credentials are configured.
	if p.config.Bucket != "" && p.config.Region != "" {
		s3Sink, err := s3sink.NewS3Sink(*p.config, nil)
		if err != nil {
			return nil, nil, err
		}
		p.s3Sink = s3Sink
	}

	// Resolve the per-thread schema (normalized only) before building the writer so
	// its arrow schema reflects it; denormalized uses the stream schema directly.
	out := any(p.schema)
	if p.stream.NormalizationEnabled() {
		if schema != nil {
			fields, ok := schema.(typeutils.Fields)
			if !ok {
				return nil, nil, fmt.Errorf("failed to typecast schema[%T] into typeutils.Fields", schema)
			}
			p.schema = fields.Clone()
			out = fields
		} else {
			fields := make(typeutils.Fields)
			fields.FromSchema(stream.Schema(), stream.ResolveColumnName)
			p.schema = fields.Clone()
			out = fields
		}
	}

	p.generations = []*schemaGen{p.newGeneration()}

	return out, nil, nil
}
