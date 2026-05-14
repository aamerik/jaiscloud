package clock

import "time"

// Clock abstracts time so providers can be tested deterministically.
// All implementations return UTC; AWS APIs expect timestamps in UTC on the wire.
type Clock interface {
	Now() time.Time
}

// RealClock returns the actual wall-clock time in UTC.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }

// FixedClock always returns the same instant (frozen time) in UTC.
type FixedClock struct {
	T time.Time
}

func (f FixedClock) Now() time.Time { return f.T.UTC() }

// OffsetClock returns wall time offset from a base instant in UTC.
// Useful for deterministic tests where time still advances, but
// starts at a known point.
type OffsetClock struct {
	base   time.Time
	origin time.Time
}

func NewOffsetClock(base time.Time) *OffsetClock {
	return &OffsetClock{base: base.UTC(), origin: time.Now()}
}

func (o *OffsetClock) Now() time.Time {
	return o.base.Add(time.Since(o.origin))
}
