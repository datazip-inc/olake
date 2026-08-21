package logger

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Span accumulates call count, wall time and an optional unit payload (records, bytes) for one
// hot-path stage. Counters are sampled on an interval instead of logged per call.
type Span struct {
	name  string
	mu    sync.Mutex
	calls int64
	nanos int64
	units int64

	prevCalls int64
	prevNanos int64
	prevUnits int64
}

var (
	spanMu    sync.Mutex
	spanList  []*Span
	spanIndex = map[string]*Span{}
)

// NewSpan registers a counter under name. Safe to call from package-level var initialisers.
func NewSpan(name string) *Span {
	spanMu.Lock()
	defer spanMu.Unlock()
	if s, ok := spanIndex[name]; ok {
		return s
	}
	s := &Span{name: name}
	spanIndex[name] = s
	spanList = append(spanList, s)
	return s
}

// Mark returns a start stamp for a timed region, or 0 when profiling is off so the
// paired Done is a no-op and the hot path stays allocation-free.
func Mark() int64 {
	if !timingEnabled {
		return 0
	}
	return time.Now().UnixNano()
}

// Done closes a region opened by Mark.
func (s *Span) Done(start int64) { s.DoneN(start, 0) }

// DoneN closes a region opened by Mark and adds units (records, bytes) to the counter.
func (s *Span) DoneN(start, units int64) {
	if start == 0 {
		return
	}
	d := time.Now().UnixNano() - start
	s.mu.Lock()
	s.calls++
	s.nanos += d
	s.units += units
	s.mu.Unlock()
}

// Count records an untimed occurrence carrying units.
func (s *Span) Count(units int64) {
	if !timingEnabled {
		return
	}
	s.mu.Lock()
	s.calls++
	s.units += units
	s.mu.Unlock()
}

func (s *Span) sample(delta bool) (calls, nanos, units int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls, nanos, units = s.calls, s.nanos, s.units
	if delta {
		calls, nanos, units = calls-s.prevCalls, nanos-s.prevNanos, units-s.prevUnits
	}
	s.prevCalls, s.prevNanos, s.prevUnits = s.calls, s.nanos, s.units
	return
}

var profileStart = time.Now()

// LogProfile emits one line per registered span. delta=true reports the interval since the
// previous call (regime changes mid-run), delta=false the whole-run totals.
func LogProfile(tag string, delta bool) {
	if !timingEnabled {
		return
	}
	spanMu.Lock()
	spans := make([]*Span, len(spanList))
	copy(spans, spanList)
	spanMu.Unlock()
	sort.Slice(spans, func(i, j int) bool { return spans[i].name < spans[j].name })

	parts := make([]string, 0, len(spans))
	for _, s := range spans {
		calls, nanos, units := s.sample(delta)
		if calls == 0 && units == 0 {
			continue
		}
		part := fmt.Sprintf("%s=%s/%d", s.name, time.Duration(nanos).Round(time.Millisecond), calls)
		if units != 0 {
			// mean units per call is what separates "few big batches" from "many small ones"
			part += fmt.Sprintf("/u%d(%.0f)", units, float64(units)/float64(max64(calls, 1)))
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	Infof("[%s] t=%.0fs go{heap=%dMB sys=%dMB gc=%d pause=%dms goroutines=%d} %s",
		tag, time.Since(profileStart).Seconds(),
		ms.HeapAlloc>>20, ms.HeapSys>>20, ms.NumGC,
		ms.PauseTotalNs/uint64(time.Millisecond), runtime.NumGoroutine(),
		strings.Join(parts, " "))
}

// LogProfileConst emits a one-shot line of per-run constants, the values that are chosen once
// and then hold for the whole sync (batch sizing, dedup mode, parallelism).
func LogProfileConst(scope string, kv ...any) {
	if !timingEnabled {
		return
	}
	parts := make([]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		parts = append(parts, fmt.Sprintf("%v=%v", kv[i], kv[i+1]))
	}
	Infof("[profile-const] %s gomaxprocs=%d numcpu=%d %s", scope,
		runtime.GOMAXPROCS(0), runtime.NumCPU(), strings.Join(parts, " "))
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
