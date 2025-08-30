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

package safego

import (
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
)

const defaultRestartTimeout = 2 * time.Second

type RecoverHandler func(value interface{})

var GlobalRecoverHandler RecoverHandler = func(_ interface{}) {}

var (
	startTime time.Time
)

type Execution struct {
	f              func()
	recoverHandler RecoverHandler
	restartTimeout time.Duration
}

// Run runs a new goroutine and add panic handler (without restart)
func Run(f func()) *Execution {
	exec := Execution{
		f:              f,
		recoverHandler: GlobalRecoverHandler,
		restartTimeout: 0,
	}
	return exec.run()
}

// RunWithRestart run a new goroutine and add panic handler:
// write logs, wait 2 seconds and restart the goroutine
func RunWithRestart(f func()) *Execution {
	exec := Execution{
		f:              f,
		recoverHandler: GlobalRecoverHandler,
		restartTimeout: defaultRestartTimeout,
	}
	return exec.run()
}

func (exec *Execution) run() *Execution {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				exec.recoverHandler(r)

				if exec.restartTimeout > 0 {
					time.Sleep(exec.restartTimeout)
					exec.run()
				}
			}
		}()
		exec.f()
	}()
	return exec
}

func (exec *Execution) WithRestartTimeout(timeout time.Duration) *Execution {
	exec.restartTimeout = timeout
	return exec
}

func Recovery(exit bool) {
	err := recover()
	if err != nil {
		logger.Error(err)
		// capture stacks trace
		for _, str := range strings.Split(string(debug.Stack()), "\n") {
			logger.Error(strings.ReplaceAll(str, "\t", ""))
		}
	}
	if exit {
		logger.Infof("Time of execution %v", time.Since(startTime).String())
		os.Exit(1)
	}
}

func Insert[T any](ch chan<- T, value T) bool {
	safeInsert := false
	func() {
		defer Recovery(false)
		ch <- value
		safeInsert = true
	}()

	return safeInsert
}

func Close[T any](ch chan T) {
	Run(func() {
		close(ch)
	})
}

func init() {
	startTime = time.Now()
}
