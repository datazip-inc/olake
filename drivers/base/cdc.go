package base

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

func (d *Driver) RunChangeStream(ctx context.Context, sd protocol.Driver, pool *protocol.WriterPool, streams ...protocol.Stream) error {
	// run pre cdc of drivers
	if err := sd.PreCDC(ctx, streams); err != nil {
		return fmt.Errorf("failed in pre cdc run for driver[%s]: %s", sd.Type(), err)
	}

	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)
	err := utils.ForEach(streams, func(stream protocol.Stream) error {
		if !d.State.HasCompletedBackfill(stream.Self()) {
			// remove chunks state
			err := d.Backfill(ctx, sd, backfillWaitChannel, pool, stream)
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
			backfilledStreams = append(backfilledStreams, streamID)

			// run parallel change stream
			// TODO: remove duplicate code
			if isParallelChangeStream(sd.Type()) {
				protocol.GlobalConnGroup.Add(func(ctx context.Context) error {
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
						_ = sd.PostCDC(ctx, streams[index], err == nil)
					}()
					return sd.StreamChanges(ctx, streams[index], func(change protocol.CDCChange) error {
						pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
						opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
						return inserter.Insert(types.CreateRawRecord(
							utils.GetKeysHash(change.Data, pkFields...),
							change.Data,
							opType,
							change.Timestamp.Time,
						))
					})
				})
			} else {
				d.State.SetGlobal(nil, streamID)
			}
		}
	}
	if isParallelChangeStream(sd.Type()) {
		// parallel change streams already processed
		return nil
	}
	protocol.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
		// Set up inserters for each stream
		inserters := make(map[protocol.Stream]*protocol.ThreadEvent)
		errChans := make(map[protocol.Stream]chan error)
		err = utils.ForEach(streams, func(stream protocol.Stream) error {
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
			_ = sd.PostCDC(ctx, nil, err == nil)
		}()
		return sd.StreamChanges(ctx, nil, func(change protocol.CDCChange) error {
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
