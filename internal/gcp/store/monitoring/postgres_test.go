//go:build gcp_persistence

package monitoring

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresDistributionRoundTrip verifies a DISTRIBUTION point value survives
// a Postgres write/read and a Snapshot/Restore round trip (the points column is
// JSONB, so the new distribution shape must marshal through it intact).
func TestPostgresDistributionRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres monitoring test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	defer s.Reset(ctx)

	base := time.Now().UTC().Truncate(time.Microsecond)
	dist := &Distribution{
		Count:                 6,
		Mean:                  3.5,
		SumOfSquaredDeviation: 17.5,
		Range:                 &DistributionRange{Min: 0, Max: 10},
		BucketOptions: &BucketOptions{
			Linear: &LinearBuckets{NumFiniteBuckets: 3, Width: 2, Offset: 1},
		},
		BucketCounts: []int64{1, 2, 2, 1, 0},
	}
	ts := TimeSeries{
		MetricType:   "custom.googleapis.com/dist",
		ResourceType: "global",
		Points:       []Point{{EndTime: base, Value: TypedValue{DistributionValue: dist}}},
	}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatalf("create: %v", err)
	}

	assertDist := func(t *testing.T, store *PostgresStore) {
		t.Helper()
		got, err := store.ListTimeSeries(ctx, "p1")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(got) != 1 || len(got[0].Points) != 1 {
			t.Fatalf("series = %+v", got)
		}
		if !reflect.DeepEqual(got[0].Points[0].Value.DistributionValue, dist) {
			t.Fatalf("distribution = %+v, want %+v", got[0].Points[0].Value.DistributionValue, dist)
		}
	}
	assertDist(t, s)

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Restore deletes and re-inserts the whole table.
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	assertDist(t, s)
}
