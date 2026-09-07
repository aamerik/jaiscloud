package firestore

import (
	"context"
	"testing"
)

// TestReset_ClearsChangeFeed verifies that Reset wipes the change-feed
// history and sequence, not just transaction read-sets. Before this fix,
// ChangesSince kept replaying pre-reset events (referencing documents the
// just-reset document store no longer has) because Reset only cleared
// readSets.
func TestReset_ClearsChangeFeed(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.Service.CreateDocument(ctx, "proj", "(default)", "documents/cities", "SF", nil); err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}
	if seq := p.CurrentSeq(); seq == 0 {
		t.Fatalf("expected non-zero seq after a write, got %d", seq)
	}
	if changes := p.ChangesSince(0); len(changes) != 1 {
		t.Fatalf("expected 1 change before reset, got %d", len(changes))
	}

	p.Reset(ctx)

	if seq := p.CurrentSeq(); seq != 0 {
		t.Fatalf("expected seq reset to 0, got %d", seq)
	}
	if changes := p.ChangesSince(0); len(changes) != 0 {
		t.Fatalf("expected no changes after reset, got %d: %+v", len(changes), changes)
	}

	// A resume token computed from the pre-reset seq must not replay
	// anything either, now that history has been wiped.
	if changes := p.ChangesSince(1); len(changes) != 0 {
		t.Fatalf("expected no changes replaying a pre-reset seq, got %d: %+v", len(changes), changes)
	}
}
