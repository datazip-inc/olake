package base

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

type CDCChange struct {
	Stream    protocol.Stream
	Timestamp typeutils.Time
	State     any
	Kind      string
	Schema    string
	Table     string
	Data      map[string]interface{}
}

type MessageProcessingFunc func(message CDCChange) error
type StreamerFunc func(ctx context.Context, callback MessageProcessingFunc) error
type PostCDCFunc func(ctx context.Context, noErr bool) error
type BackfillFunc func(ctx context.Context, backfillWaitChannel chan struct{}, pool *protocol.WriterPool, stream protocol.Stream) error

func (d *Driver) RunChangeStream(ctx context.Context, backfill BackfillFunc, streamer StreamerFunc, postCDC PostCDCFunc, pool *protocol.WriterPool, streams ...protocol.Stream) error {
	backfillWaitChannel := make(chan struct{}, 1)
	defer close(backfillWaitChannel)
	err := utils.ForEach(streams, func(stream protocol.Stream) error {
		if !d.State.HasCompletedBackfill(stream.Self()) {
			err := backfill(ctx, backfillWaitChannel, pool, stream)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to run backfill: %s", err)
	}

	// wait for backfill to finish
	// <-backfillWaitChannel

	// handle drivers which have parallel change streams
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
			if err == nil {
				for stream, insert := range inserters {
					insert.Close()
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

// func isParallelChangeStream(driverType constants.DriverType) bool {
// 	return driverType == constants.MongoDB
// }
