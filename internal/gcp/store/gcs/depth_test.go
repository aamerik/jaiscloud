package gcs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStoreVersioningGenerations(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	now := time.Now()
	gen1 := ObjectMeta{Bucket: "bkt", Name: "o", Generation: "10", Size: 1, TimeCreated: now, Updated: now}
	gen2 := ObjectMeta{Bucket: "bkt", Name: "o", Generation: "11", Size: 2, TimeCreated: now.Add(time.Second), Updated: now.Add(time.Second)}

	if err := s.PutObjectGeneration(ctx, "bkt", "o", gen1); err != nil {
		t.Fatalf("put gen1: %v", err)
	}
	if err := s.PutObjectGeneration(ctx, "bkt", "o", gen2); err != nil {
		t.Fatalf("put gen2: %v", err)
	}

	// Live generation is the newest.
	live, err := s.GetObjectMeta(ctx, "bkt", "o")
	if err != nil || live.Generation != "11" {
		t.Fatalf("expected live generation 11, got %+v / %v", live, err)
	}

	// The archived generation is readable by id and carries timeDeleted.
	old, err := s.GetObjectGeneration(ctx, "bkt", "o", "10")
	if err != nil || old.Generation != "10" || old.TimeDeleted == nil {
		t.Fatalf("expected archived generation 10 with timeDeleted, got %+v / %v", old, err)
	}

	// A missing generation returns ErrNoSuchObject.
	if _, err := s.GetObjectGeneration(ctx, "bkt", "o", "99"); !errors.Is(err, ErrNoSuchObject) {
		t.Fatalf("expected ErrNoSuchObject for missing generation, got %v", err)
	}

	// Live listing sees only the newest generation.
	liveList, err := s.ListObjects(ctx, "bkt")
	if err != nil || len(liveList) != 1 || liveList[0].Generation != "11" {
		t.Fatalf("expected 1 live object (gen 11), got %+v / %v", liveList, err)
	}

	// Versions listing sees both generations.
	vers, err := s.ListObjectVersions(ctx, "bkt")
	if err != nil || len(vers) != 2 {
		t.Fatalf("expected 2 versions, got %+v / %v", vers, err)
	}
}

func TestStoreObjectRetentionHoldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	until := time.Now().Add(time.Hour).UTC()
	o := ObjectMeta{
		Bucket: "bkt", Name: "r", Generation: "1",
		Retention:     &ObjectRetention{RetainUntilTime: until, Mode: "Unlocked"},
		TemporaryHold: true,
		TimeCreated:   time.Now(), Updated: time.Now(),
	}
	if err := s.PutObjectMeta(ctx, "bkt", "r", o); err != nil {
		t.Fatalf("put object: %v", err)
	}
	got, err := s.GetObjectMeta(ctx, "bkt", "r")
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if got.Retention == nil || got.Retention.Mode != "Unlocked" || !got.Retention.RetainUntilTime.Equal(until) {
		t.Fatalf("retention not round-tripped: %+v", got.Retention)
	}
	if !got.TemporaryHold {
		t.Fatal("expected temporaryHold true")
	}
}
