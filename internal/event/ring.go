package event

import "eventpush/internal/model"

// Ring is the fixed-capacity circular buffer backing the store.
type Ring struct {
	buf   []model.Event
	head  int
	count int
	cap   int
}

// NewRing creates a circular buffer with the given capacity.
func NewRing(capacity int) *Ring {
	if capacity < 1 {
		capacity = 1
	}
	return &Ring{
		buf: make([]model.Event, capacity),
		cap: capacity,
	}
}

// Push appends an event, reporting false when the buffer is full.
func (r *Ring) Push(ev model.Event) bool {
	if r.count == r.cap {
		return false
	}
	r.buf[(r.head+r.count)%r.cap] = ev
	r.count++
	return true
}

// Get returns the event with the exact sequence, if retained.
func (r *Ring) Get(seq int64) (model.Event, bool) {
	for i := 0; i < r.count; i++ {
		ev := r.buf[(r.head+i)%r.cap]
		if ev.Sequence == seq {
			return ev, true
		}
	}
	return model.Event{}, false
}

// Each visits every retained event in insertion order.
func (r *Ring) Each(fn func(model.Event) bool) {
	for i := 0; i < r.count; i++ {
		if !fn(r.buf[(r.head+i)%r.cap]) {
			return
		}
	}
}

// Len returns the number of retained events.
func (r *Ring) Len() int {
	return r.count
}
