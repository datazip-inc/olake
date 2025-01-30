package base

import (
	"time"

	"github.com/datazip-inc/olake/logger"
)

func RetryOnBackoff(attempts int, sleep time.Duration, f func() error) (err error) {
	for cur := 1; cur <= attempts; cur++ {
		if err = f(); err == nil {
			return nil
		}
		logger.Infof("Retrying after %f seconds due to err: %s", sleep.Seconds(), err)
		time.Sleep(sleep)
		sleep = sleep * 2
	}

	return err
}
