package abstract

import (
	"strings"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func RetryOnBackoff(attempts int, sleep time.Duration, f func() error) (err error) {
	for cur := 0; cur < attempts; cur++ {
		if err = f(); err == nil {
			return nil
		}
		if strings.Contains(err.Error(), destination.DestError) || strings.Contains(err.Error(), "context canceled") {
			break // if destination error or global context canceled, break the retry loop
		}
		if attempts > 1 && cur != attempts-1 {
			logger.Infof("retry attempt[%d], retrying after %.2f seconds due to err: %s", cur+1, sleep.Seconds(), err)
			time.Sleep(sleep)
			sleep = sleep * 2
		}
	}

	return err
}

func dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, DBTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}
