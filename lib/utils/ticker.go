package utils

import "time"

type Ticker struct {
	C       <-chan time.Time
	stop    chan struct{}
	stopped chan struct{}
}

func NewTicker(initialDelay, interval time.Duration) *Ticker {
	ch := make(chan time.Time, 1)
	stop := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(ch)
		defer close(stopped)

		// Initial delay.
		if initialDelay > 0 {
			timer := time.NewTimer(initialDelay)
			select {
			case t := <-timer.C:
				ch <- t
			case <-stop:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		} else {
			select {
			case ch <- time.Now():
			case <-stop:
				return
			}
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case t := <-ticker.C:
				select {
				case ch <- t:
				case <-stop:
					return
				}
			case <-stop:
				return
			}
		}
	}()

	return &Ticker{
		C:       ch,
		stop:    stop,
		stopped: stopped,
	}
}

func (t *Ticker) Stop() {
	close(t.stop)
	<-t.stopped
}
