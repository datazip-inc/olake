package safego

import (
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/datazip-inc/olake/logger"
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
