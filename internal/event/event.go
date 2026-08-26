// Package event implements the in-memory event store and the commit
// cursor used by the publish path.
package event

import (
	"errors"
	"sync"

	"eventpush/internal/model"
)

// ErrRingFull is returned when the bounded store cannot accept another
// event.
var ErrRingFull = errors.New("event store is full")

// ErrInvalidSequence is returned when a commit targets a sequence that
// was never appended.
var ErrInvalidSequence = errors.New("commit sequence was never appended")

// Store is a bounded, sequence-ordered event buffer. Append assigns the
// next sequence and leaves the event pending; Commit advances the
// visible cursor only after fanout has confirmed delivery.
type Store struct {
	mu        sync.Mutex
	ring      *Ring
	seq       int64
	committed int64
}

// NewStore creates a store with the given capacity.
func NewStore(capacity int) *Store {
	return &Store{ring: NewRing(capacity)}
}

// Append assigns the next sequence and stores the event as pending.
func (s *Store) Append(ev model.Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	ev.Sequence = s.seq
	if !s.ring.Push(ev) {
		return 0, ErrRingFull
	}
	s.committed = s.seq
	return s.seq, nil
}

// Commit advances the committed cursor to seq. The cursor never moves
// backwards.
func (s *Store) Commit(seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.seq {
		return ErrInvalidSequence
	}
	if seq > s.committed {
		s.committed = seq
	}
	return nil
}

// Cursor returns the highest committed sequence.
func (s *Store) Cursor() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// Since returns committed events with a sequence greater than after.
func (s *Store) Since(after int64) ([]model.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.Event
	for seq := after + 1; seq <= s.committed; seq++ {
		if ev, ok := s.ring.Get(seq); ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

// Pending returns events that were appended but not yet committed;
// these are the candidates for a retry after a failed dispatch.
func (s *Store) Pending() []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.Event
	s.ring.Each(func(ev model.Event) bool {
		if ev.Sequence > s.committed {
			out = append(out, ev)
		}
		return true
	})
	return out
}

// Len returns how many events are currently retained.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring.Len()
}
