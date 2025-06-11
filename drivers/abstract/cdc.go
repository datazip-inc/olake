package abstract

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (a *AbstractDriver) RunChangeStream(ctx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	// run pre cdc of drivers
	if err := a.driver.PreCDC(ctx, streams); err != nil {
		return fmt.Errorf("failed in pre cdc run for driver[%s]: %s", a.driver.Type(), err)
	}

	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)
	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		if !a.state.HasCompletedBackfill(stream.Self()) {
			// remove chunks state
			err := a.Backfill(ctx, backfillWaitChannel, pool, stream)
			if err != nil {
				return err
			}
		} else {
			logger.Infof("backfill already completed for stream[%s], skipping", stream.ID())
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
			// if main context stuck in error
			return ctx.Err()
		case <-a.GlobalConnGroup.Ctx().Done():
			// if global conn group stuck in error
			return nil
		case streamID, ok := <-backfillWaitChannel:
			if !ok {
				return fmt.Errorf("backfill channel closed unexpectedly")
			}
			backfilledStreams = append(backfilledStreams, streamID)

			// run parallel change stream
			// TODO: remove duplicate code
			if isParallelChangeStream(a.driver.Type()) {
				a.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
					index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })
					errChan := make(chan error, 1)
					inserter := pool.NewThread(ctx, streams[index], errChan)
					defer func() {
						inserter.Close()
						if threadErr := <-errChan; threadErr != nil {
							err = fmt.Errorf("failed to insert cdc record of stream %s, insert func error: %s, thread error: %s", streamID, err, threadErr)
						}
						postCDCErr := a.driver.PostCDC(ctx, streams[index], err == nil)
						if postCDCErr != nil {
							err = fmt.Errorf("post cdc error: %s, cdc insert thread error: %s", postCDCErr, err)
						}
					}()
					return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
						return a.driver.StreamChanges(ctx, streams[index], func(change CDCChange) error {
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
				})
			} else {
				a.state.SetGlobal(nil, streamID)
			}
		}
	}
	if isParallelChangeStream(a.driver.Type()) {
		// parallel change streams already processed
		return nil
	}
	// TODO: For a big table cdc (for all tables) will not start until backfill get finished, need to study alternate ways to do cdc sync
	a.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
		// Set up inserters for each stream
		inserters := make(map[types.StreamInterface]*destination.ThreadEvent)
		errChans := make(map[types.StreamInterface]chan error)
		err = utils.ForEach(streams, func(stream types.StreamInterface) error {
			errChan := make(chan error, 1)
			inserters[stream], errChans[stream] = pool.NewThread(ctx, stream, errChan), errChan
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to create writer thread: %s", err)
		}
		defer func() {
			for stream, insert := range inserters {
				insert.Close()
				if threadErr := <-errChans[stream]; threadErr != nil {
					err = fmt.Errorf("failed to insert cdc record of stream %s, insert func error: %s, thread error: %s", stream.ID(), err, threadErr)
				}
			}
			postCDCErr := a.driver.PostCDC(ctx, nil, err == nil)
			if postCDCErr != nil {
				err = fmt.Errorf("post cdc error: %s, cdc insert thread error: %s", postCDCErr, err)
			}
		}()
		return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
			return a.driver.StreamChanges(ctx, nil, func(change CDCChange) error {
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
	})
	return nil
}

func isParallelChangeStream(driverType string) bool {
	return driverType == string(constants.MongoDB)
}
