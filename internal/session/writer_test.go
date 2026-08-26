package session

import (
	"testing"
	"time"

	"eventpush/internal/model"
)

func TestWriterEnqueueAndShutdown(t *testing.T) {
	w := NewWriter(plainConn{}, 4, 100*time.Millisecond)
	ev := model.NewEvent("t", []byte("x"))
	ev.Sequence = 1
	if err := w.Enqueue(ev); err != nil {
		t.Fatal(err)
	}
	w.Shutdown()
	// Shutdown must return promptly; the transport is released by the
	// writer regardless of queue state.
}

func TestEncodeFrame(t *testing.T) {
	ev := model.NewEvent("t", []byte("v"))
	ev.Sequence = 9
	if got, want := string(Encode(ev)), "9|t|v"; got != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
}
