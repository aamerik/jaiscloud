package storeutil

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// SerializableMaxAttempts bounds retries of a Postgres transaction that fails
// with a transient serialization conflict. A handful of attempts with a short
// linear backoff is ample for the write skew the GCP stores see (concurrent
// JSON-API and executor/reconciler writers touching the same row); a persistent
// conflict still surfaces to the caller rather than looping forever.
const SerializableMaxAttempts = 5

// serializableBackoffUnit is the linear backoff increment between attempts.
const serializableBackoffUnit = 2 * time.Millisecond

// IsSerializationFailure reports whether err is a transient Postgres
// transaction conflict that is safe to retry: serialization_failure (40001),
// raised when concurrent read/write sets conflict under SERIALIZABLE, or
// deadlock_detected (40P01), raised when two transactions lock rows in opposite
// orders. Both mean "retry the whole transaction", not "the request is
// invalid" — surfacing them as a 500 makes a benign race look like a crash.
//
// This mirrors the GCS object store's internal retry (store/gcs/retry.go);
// it is shared here so every Postgres store's atomic read-modify-write method
// gets the same guarantee instead of each re-implementing it.
func IsSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40001" || pgErr.Code == "40P01"
}

// RetrySerializable runs fn, re-running it while it returns a transient
// serialization conflict, up to SerializableMaxAttempts times with a short
// linear backoff. fn must be a self-contained transaction that rolls back (and
// returns) on failure so re-running it is safe: every caller opens its own
// transaction, defers Rollback, and returns without committing on error.
func RetrySerializable[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	for attempt := 1; ; attempt++ {
		v, err := fn()
		if err == nil || !IsSerializationFailure(err) || attempt >= SerializableMaxAttempts {
			return v, err
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(time.Duration(attempt) * serializableBackoffUnit):
		}
	}
}
