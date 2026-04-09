package clock

import "time"

// Clock abstracts time so providers can be tested deterministically.
type Clock interface {
	Now() time.Time
}

// RealClock returns the actual wall-clock time.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FixedClock always returns the same instant (frozen time).
type FixedClock struct {
	T time.Time
}

func (f FixedClock) Now() time.Time { return f.T }

// OffsetClock returns wall time offset from a base instant.
// Useful for deterministic tests where time still advances, but
// starts at a known point.
type OffsetClock struct {
	base   time.Time
	origin time.Time
}

func NewOffsetClock(base time.Time) *OffsetClock {
	return &OffsetClock{base: base, origin: time.Now()}
}

func (o *OffsetClock) Now() time.Time {
	return o.base.Add(time.Since(o.origin))
}
