package abstract

import (
	"strings"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/utils/logger"
)

func RetryOnBackoff(attempts int, sleep time.Duration, f func() error) (err error) {
	for cur := 0; cur < attempts; cur++ {
		if err = f(); err == nil {
			return nil
		}
		if strings.Contains(err.Error(), destination.DestError) {
			break // if destination error, break the retry loop
		}
		if attempts > 1 && cur != attempts-1 {
			logger.Infof("retry attempt[%d], retrying after %.2f seconds due to err: %s", cur+1, sleep.Seconds(), err)
			time.Sleep(sleep)
			sleep = sleep * 2
		}
	}

	return err
}
