/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
