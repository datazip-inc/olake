package abstract

import (
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/logger"
)

func RetryOnBackoff(attempts int, sleep time.Duration, f func() error) (err error) {
	for cur := 0; cur < attempts; cur++ {
		if err = f(); err == nil {
			return nil
		}

		// check if error is non retryable
		for _, nonRetryableError := range constants.NonRetryableErrors {
			if strings.Contains(err.Error(), nonRetryableError) {
				break
			}
		}

		if attempts > 1 && cur != attempts-1 {
			logger.Infof("retry attempt[%d], retrying after %.2f seconds due to err: %s", cur+1, sleep.Seconds(), err)
			time.Sleep(sleep)
			sleep = sleep * 2
		}
	}

	return err
}
