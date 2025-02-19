package base

import (
	"time"

	"github.com/datazip-inc/olake/logger"
)

func RetryOnBackoff(attempts int, sleep time.Duration, f func() error) (err error) {
	for cur := 0; cur < attempts; cur++ {
		if err = f(); err == nil {
			return nil
		}
		logger.Infof("retry attempt[%d], retrying after %.2f seconds due to err: %s", cur+1, sleep.Seconds(), err)
		time.Sleep(sleep)
		sleep = sleep * 2
	}

	return err
}
