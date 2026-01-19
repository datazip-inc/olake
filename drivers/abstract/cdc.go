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
		isStrictCDC := stream.GetStream().SyncMode == types.STRICTCDC
		if a.state.HasCompletedBackfill(stream.Self()) || isStrictCDC {
			logger.Infof("backfill %s for stream[%s], skipping", utils.Ternary(isStrictCDC, "not enabled", "completed").(string), stream.ID())
			backfillWaitChannel <- stream.ID()
		} else {
			err := a.Backfill(ctx, backfillWaitChannel, pool, stream)
			if err != nil {
				return err
			}
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
					threadID := fmt.Sprintf("%s_%s", streams[index].ID(), utils.ULID())
					inserter, err := pool.NewWriter(ctx, streams[index], destination.WithThreadID(threadID), destination.WithDriverType(constants.DriverType(a.driver.Type())))
					if err != nil {
						return fmt.Errorf("failed to create new thread in pool, error: %s", err)
					}
					logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, streams[index].ID())
					defer func() {
						if threadErr := inserter.Close(ctx); threadErr != nil {
							err = fmt.Errorf("failed to insert cdc record of stream %s, insert func error: %s, thread error: %s", streamID, err, threadErr)
						}

						// check for panics before saving state
						if r := recover(); r != nil {
							err = fmt.Errorf("panic recovered in cdc: %v, prev error: %s", r, err)
						}

						postCDCErr := a.driver.PostCDC(ctx, streams[index], err == nil, "")
						if postCDCErr != nil {
							err = fmt.Errorf("post cdc error: %s, cdc insert thread error: %s", postCDCErr, err)
						}

						if err != nil {
							err = fmt.Errorf("thread[%s]: %s", threadID, err)
						}
					}()
					return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
						return a.driver.StreamChanges(ctx, streams[index], func(ctx context.Context, change CDCChange) error {
							pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
							opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
							return inserter.Push(ctx, types.CreateRawRecord(
								utils.GetKeysHash(change.Data, pkFields...),
								change.Data,
								opType,
								&change.Timestamp,
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

	// handle for kafka differently
	kafkaDriver, ok := a.GetKafkaInterface()
	if ok {
		// set limit to global connection group according to readers available
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, len(kafkaDriver.GetReaderIDs()))
		utils.ConcurrentInGroup(a.GlobalConnGroup, kafkaDriver.GetReaderIDs(), func(ctx context.Context, readerID string) (err error) {
			inserters := make(map[string]*destination.WriterThread)
			defer func() {
				for streamID, inserter := range inserters {
					if threadErr := inserter.Close(ctx); threadErr != nil {
						err = fmt.Errorf("failed closing writer[%s]: %s", streamID, threadErr)
					}
				}

				if r := recover(); r != nil {
					err = fmt.Errorf("panic recovered in cdc: %s, prev error: %v", r, err)
				}

				postCDCErr := a.driver.PostCDC(ctx, nil, err == nil, readerID)
				if postCDCErr != nil {
					err = fmt.Errorf("post cdc error: %s, cdc insert thread error: %v", postCDCErr, err)
				}
			}()
			return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
				return kafkaDriver.PartitionStreamChanges(ctx, readerID, func(ctx context.Context, message CDCChange) error {
					inserter := inserters[message.Stream.ID()]
					if inserter == nil {
						// reader can read from multiple streams in kafka which we cant decide (r1 -> s1, s2, r2 -> s2, s3)
						// so creating writer for stream which read by reader in kafka
						threadID := fmt.Sprintf("%s_%s", readerID, message.Stream.ID())
						inserter, err = pool.NewWriter(ctx, message.Stream, destination.WithThreadID(threadID), destination.WithDriverType(constants.DriverType(a.driver.Type())))
						if err != nil {
							return fmt.Errorf("failed to create writer for stream %s: %s", message.Stream.ID(), err)
						}
						inserters[message.Stream.ID()] = inserter
						logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, message.Stream.ID())
					}
					return inserter.Push(ctx, types.CreateRawRecord(
						utils.GetKeysHash(message.Data, message.Stream.GetStream().SourceDefinedPrimaryKey.Array()...),
						message.Data,
						"c", // kafka message is always a create operation
						&message.Timestamp,
					))
				})
			})
		})
		return nil
	}

	// TODO: For a big table cdc (for all tables) will not start until backfill get finished, need to study alternate ways to do cdc sync
	a.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
		// Set up inserters for each stream
		inserters := make(map[types.StreamInterface]*destination.WriterThread)
		err = utils.ForEach(streams, func(stream types.StreamInterface) error {
			threadID := fmt.Sprintf("%s_%s", stream.ID(), utils.ULID())
			inserters[stream], err = pool.NewWriter(ctx, stream, destination.WithThreadID(threadID), destination.WithDriverType(constants.DriverType(a.driver.Type())))
			if err != nil {
				logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, stream.ID())
			}
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to create writer thread: %s", err)
		}
		defer func() {
			for stream, insert := range inserters {
				if threadErr := insert.Close(ctx); threadErr != nil {
					err = fmt.Errorf("failed to insert cdc record of stream %s, insert func error: %s, thread error: %s", stream.ID(), err, threadErr)
				}
			}

			// check for panics before saving state
			if r := recover(); r != nil {
				err = fmt.Errorf("panic recovered in cdc: %v, prev error: %s", r, err)
			}

			postCDCErr := a.driver.PostCDC(ctx, nil, err == nil, "")
			if postCDCErr != nil {
				err = fmt.Errorf("post cdc error: %s, cdc insert thread error: %s", postCDCErr, err)
			}
		}()
		return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
			return a.driver.StreamChanges(ctx, nil, func(ctx context.Context, change CDCChange) error {
				pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
				opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
				return inserters[change.Stream].Push(ctx, types.CreateRawRecord(
					utils.GetKeysHash(change.Data, pkFields...),
					change.Data,
					opType,
					&change.Timestamp,
				))
			})
		})
	})
	return nil
}

func isParallelChangeStream(driverType string) bool {
	return driverType == string(constants.MongoDB)
}
