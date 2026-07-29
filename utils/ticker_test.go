package utils

import (
	"testing"
	"time"
)

func TestTickerImmediateFirstTick(t *testing.T) {
	ticker := NewTicker(0, 100*time.Millisecond)
	defer ticker.Stop()

	start := time.Now()

	select {
	case <-ticker.C:
		if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
			t.Fatalf("first tick took too long: %v", elapsed)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timed out waiting for first tick")
	}
}

func TestTickerDelayedFirstTick(t *testing.T) {
	delay := 100 * time.Millisecond

	ticker := NewTicker(delay, time.Second)
	defer ticker.Stop()

	start := time.Now()

	select {
	case <-ticker.C:
		elapsed := time.Since(start)
		if elapsed < delay-20*time.Millisecond {
			t.Fatalf("first tick arrived too early: %v", elapsed)
		}
	case <-time.After(delay + 200*time.Millisecond):
		t.Fatal("timed out waiting for first tick")
	}
}

func TestTickerInterval(t *testing.T) {
	interval := 50 * time.Millisecond

	ticker := NewTicker(0, interval)
	defer ticker.Stop()

	<-ticker.C // immediate tick

	start := time.Now()

	select {
	case <-ticker.C:
		elapsed := time.Since(start)
		if elapsed < interval-20*time.Millisecond {
			t.Fatalf("second tick arrived too early: %v", elapsed)
		}
	case <-time.After(interval + 100*time.Millisecond):
		t.Fatal("timed out waiting for second tick")
	}
}

func TestTickerStop(t *testing.T) {
	ticker := NewTicker(0, 10*time.Millisecond)

	<-ticker.C // consume initial tick

	ticker.Stop()

	for {
		select {
		case _, ok := <-ticker.C:
			if !ok {
				return // success
			}
			// Drain any buffered tick.
		case <-time.After(100 * time.Millisecond):
			t.Fatal("channel was not closed after Stop")
		}
	}
}
