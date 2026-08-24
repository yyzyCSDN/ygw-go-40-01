package ack

import (
	"testing"

	"eventpush/internal/model"
)

func TestTrackerConfirmAdvancesSnapshot(t *testing.T) {
	tr := NewTracker(model.SessionID("s1"))
	tr.Record(5, true)
	if !tr.Pending(5) {
		t.Fatal("sequence 5 should be pending after delivery")
	}
	tr.Confirm(5)
	if tr.Snapshot() != 5 {
		t.Fatalf("snapshot = %d, want 5", tr.Snapshot())
	}
	if tr.Pending(5) {
		t.Fatal("sequence 5 should not stay pending after confirm")
	}
	if !tr.Covered(4) {
		t.Fatal("confirmed window should cover earlier sequences")
	}
}

func TestTrackerOwnerBound(t *testing.T) {
	tr := NewTracker(model.SessionID("s2"))
	if tr.Owner() != model.SessionID("s2") {
		t.Fatalf("owner = %q, want s2", tr.Owner())
	}
}
