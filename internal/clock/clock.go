package clock

import (
	"sync/atomic"
	"time"
)

// Clock abstracts time so providers can be tested deterministically.
//
// ALL IMPLEMENTATIONS MUST RETURN UTC. This is not optional: every AWS API
// response carries timestamps on the wire in UTC (ISO 8601 / Unix epoch). A
// provider that uses a non-UTC time.Time produces timestamps that are wrong for
// clients outside the UTC timezone. Replacing bare time.Now() with clock.Now()
// therefore fixes two issues simultaneously: it routes through the global clock
// for time-freeze support, AND it normalises to UTC for AWS wire compatibility.
//
// Rule: never call time.Now() directly in provider, store, or worker code.
// Use clock.Now() instead. See CLAUDE.md §Key conventions for the full policy.
type Clock interface {
	Now() time.Time
}

// RealClock returns the actual wall-clock time in UTC.
// UTC is mandatory — AWS APIs expect timestamps in UTC on the wire.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }

// FixedClock always returns the same instant (frozen time) in UTC.
// UTC is mandatory — see the Clock interface doc above.
type FixedClock struct {
	T time.Time
}

func (f FixedClock) Now() time.Time { return f.T.UTC() }

// OffsetClock returns wall time offset from a base instant in UTC.
// The clock starts at base (stored in UTC) and advances at real wall-clock speed
// from the moment NewOffsetClock is called. Two calls N seconds apart return
// values N seconds apart — the clock is just anchored to a different epoch.
// UTC is mandatory — see the Clock interface doc above.
type OffsetClock struct {
	base   time.Time
	origin time.Time
}

func NewOffsetClock(base time.Time) *OffsetClock {
	// Real wall time: origin is the actual wall-clock instant when the offset
	// clock was created. time.Since(origin) in Now() measures real elapsed time
	// so the clock advances at real speed from the chosen base.
	return &OffsetClock{base: base.UTC(), origin: time.Now()}
}

func (o *OffsetClock) Now() time.Time {
	// Real wall time: time.Since measures actual elapsed duration since creation,
	// which is the correct behaviour for offset mode (advances at real speed).
	return o.base.Add(time.Since(o.origin))
}

// clockHolder is the uniform concrete type always stored in globalClock.
// atomic.Value panics if the concrete type of stored values changes across Store
// calls — RealClock{}, FixedClock{}, and *OffsetClock are three different types,
// so this wrapper is mandatory.
type clockHolder struct{ clk Clock }

// globalClock is the process-wide clock used by all providers, stores, and workers.
var globalClock atomic.Value

// SetGlobalClock replaces the process-wide clock.
// Pass RealClock{} (not nil) to restore wall time.
func SetGlobalClock(clk Clock) {
	if clk == nil {
		clk = RealClock{}
	}
	globalClock.Store(clockHolder{clk})
}

// Now returns the current time from the global clock, always in UTC.
//
// This is the correct way to get the current time in provider, store, and
// worker code. It routes through the global atomic clock so tests can freeze
// or offset time, and always returns UTC for AWS wire compatibility.
//
// Falls back to time.Now().UTC() if SetGlobalClock has never been called.
func Now() time.Time {
	if h, ok := globalClock.Load().(clockHolder); ok {
		return h.clk.Now()
	}
	return time.Now().UTC()
}

// RealNow returns the actual wall-clock time in UTC, bypassing the global clock.
//
// Use this instead of time.Now() when real elapsed time is required — container
// lifecycle waits, keepalive timers, GC eviction, TLS cert windows, request
// latency measurement. Calling RealNow() makes the intent explicit: "I need
// real wall time here, not the simulated clock."
//
// Never call time.Now() directly — use clock.Now() or clock.RealNow().
func RealNow() time.Time {
	return time.Now().UTC()
}
