package base

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type CDCChange struct {
	Stream    protocol.Stream
	Timestamp typeutils.Time
	Kind      string
	Data      map[string]interface{}
}

type MessageProcessingFunc func(message CDCChange) error
type StreamerFunc func(ctx context.Context, callback MessageProcessingFunc) error
type PostCDCFunc func(ctx context.Context, noErr bool) error

func (d *Driver) RunChangeStream(ctx context.Context, sd protocol.Driver, streamer StreamerFunc, postCDC PostCDCFunc, pool *protocol.WriterPool, streams ...protocol.Stream) error {
	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)
	err := utils.ForEach(streams, func(stream protocol.Stream) error {
		if !d.State.HasCompletedBackfill(stream.Self()) {
			err := sd.Backfill(ctx, backfillWaitChannel, pool, stream)
			if err != nil {
				return err
			}
		} else {
			backfillWaitChannel <- stream.ID()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to run backfill: %s", err)
	}

	// Wait for all backfill processes to complete
	backfilledStreams := make([]string, 0, len(streams))
	for len(backfilledStreams) < len(streams) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case streamID, ok := <-backfillWaitChannel:
			if !ok {
				return fmt.Errorf("backfill channel closed unexpectedly")
			}
			d.State.SetGlobal(nil, streamID)
			backfilledStreams = append(backfilledStreams, streamID)

			// run parallel change stream
			// TODO: remove duplicate code
			if isParallelChangeStream(sd.Type()) {
				index, _ := utils.ArrayContains(streams, func(s protocol.Stream) bool { return s.ID() == streamID })
				errChan := make(chan error, 1)
				inserter, err := pool.NewThread(ctx, streams[index], protocol.WithErrorChannel(errChan))
				if err != nil {
					return fmt.Errorf("failed to create writer thread for stream[%s]: %s", streamID, err)
				}
				defer func() {
					inserter.Close()
					if err == nil {
						if threadErr := <-errChan; threadErr != nil {
							err = fmt.Errorf("failed to write record for stream[%s]: %s", streamID, threadErr)
						}
					}
					_ = postCDC(ctx, err == nil)
				}()
				err = streamer(ctx, func(change CDCChange) error {
					pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
					opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
					return inserter.Insert(types.CreateRawRecord(
						utils.GetKeysHash(change.Data, pkFields...),
						change.Data,
						opType,
						change.Timestamp.Time,
					))
				})
				if err != nil {
					return fmt.Errorf("failed cdc for stream[%s]: %s", streamID, err)
				}
			}
		}
	}
	if isParallelChangeStream(sd.Type()) {
		// parallel change streams already processed
		return nil
	}
	protocol.GlobalConnGroup.Add(func(ctx context.Context) error {
		// Set up inserters for each stream
		inserters := make(map[protocol.Stream]*protocol.ThreadEvent)
		errChans := make(map[protocol.Stream]chan error)
		err := utils.ForEach(streams, func(stream protocol.Stream) error {
			errChan := make(chan error, 1)
			inserter, err := pool.NewThread(ctx, stream, protocol.WithErrorChannel(errChan))
			if err != nil {
				return fmt.Errorf("failed to create writer thread for stream[%s]: %s", stream.ID(), err)
			}
			inserters[stream], errChans[stream] = inserter, errChan
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to create writer thread: %s", err)
		}
		defer func() {
			for stream, insert := range inserters {
				insert.Close()
				if err == nil {
					if threadErr := <-errChans[stream]; threadErr != nil {
						err = fmt.Errorf("failed to write record for stream[%s]: %s", stream.ID(), threadErr)
					}
				}
			}
			_ = postCDC(ctx, err == nil)
		}()
		return streamer(ctx, func(change CDCChange) error {
			pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
			opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
			return inserters[change.Stream].Insert(types.CreateRawRecord(
				utils.GetKeysHash(change.Data, pkFields...),
				change.Data,
				opType,
				change.Timestamp.Time,
			))
		})
	})
	return nil
}

func isParallelChangeStream(driverType string) bool {
	return driverType == string(constants.MongoDB)
}
