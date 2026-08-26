package event

import (
	"testing"

	"eventpush/internal/model"
)

func TestStoreAppendAndFetch(t *testing.T) {
	s := NewStore(16)
	seq, err := s.Append(model.NewEvent("device/a", []byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("first sequence = %d, want 1", seq)
	}
	if err := s.Commit(seq); err != nil {
		t.Fatal(err)
	}
	committed, err := s.Since(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) != 1 || committed[0].Sequence != seq {
		t.Fatalf("committed = %v, want sequence %d", committed, seq)
	}
	ev := committed[0]
	if string(ev.Payload) != "hello" {
		t.Fatalf("payload = %q, want hello", ev.Payload)
	}
	if s.Len() != 1 {
		t.Fatalf("store length = %d, want 1", s.Len())
	}
}

func TestStoreRingFull(t *testing.T) {
	s := NewStore(2)
	for i := 0; i < 2; i++ {
		if _, err := s.Append(model.NewEvent("t", []byte("x"))); err != nil {
			t.Fatalf("append %d failed: %v", i+1, err)
		}
	}
	if _, err := s.Append(model.NewEvent("t", []byte("x"))); err == nil {
		t.Fatal("append beyond capacity should fail")
	}
}
