// Package ack tracks per-session delivery confirmation state. The
// tracker keeps the highest confirmed sequence and the set of
// delivered-but-unconfirmed sequences so replay can resume from the
// correct position after a reconnect.
package ack

import (
	"sync"

	"eventpush/internal/model"
)

// Tracker records delivery results and confirmations for one session
// incarnation.
type Tracker struct {
	mu      sync.Mutex
	owner   model.SessionID
	acked   int64
	pending map[int64]struct{}
	epoch   uint64
}

// NewTracker creates an empty tracker owned by the given session.
func NewTracker(owner model.SessionID) *Tracker {
	return &Tracker{
		owner:   owner,
		pending: make(map[int64]struct{}),
	}
}

// Record marks a sequence as delivered or not delivered. A failed write
// leaves the sequence out of the pending set so replay can retry it.
func (t *Tracker) Record(seq int64, delivered bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordLocked(seq, delivered)
}

func (t *Tracker) recordLocked(seq int64, delivered bool) {
	if delivered {
		t.pending[seq] = struct{}{}
		return
	}
	delete(t.pending, seq)
}

// Confirm advances the contiguous confirmed position. Late confirms for
// already-advanced positions are ignored.
func (t *Tracker) Confirm(seq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pending, seq)
	if seq > t.acked {
		t.acked = seq
	}
}

// Snapshot returns the highest contiguous confirmed sequence.
func (t *Tracker) Snapshot() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.acked
}

// Pending reports whether a sequence is delivered but unconfirmed.
func (t *Tracker) Pending(seq int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.pending[seq]
	return ok
}

// Covered reports whether a sequence is inside the confirmed window and
// therefore must not be delivered again.
func (t *Tracker) Covered(seq int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return seq <= t.acked
}

// Owner returns the session this tracker is bound to.
func (t *Tracker) Owner() model.SessionID {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.owner
}

// Reset clears all state and bumps the incarnation epoch so a stale
// tracker can never be mistaken for a fresh one.
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resetLocked()
}

func (t *Tracker) resetLocked() {
	t.acked = 0
	t.pending = make(map[int64]struct{})
	t.epoch++
}

// Epoch identifies the tracker incarnation; it changes on every Reset.
func (t *Tracker) Epoch() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.epoch
}

// Len returns how many sequences are delivered but unconfirmed.
func (t *Tracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}
