package event

// store.go holds the bounded-store housekeeping used by the status
// endpoint: the oldest retained sequence and the retry backlog depth.

// Backlog describes pending work for one store.
type Backlog struct {
	Pending   int
	Committed int64
	NextSeq   int64
	Retained  int
}

// BacklogInfo computes the current backlog of a store.
func BacklogInfo(s *Store) Backlog {
	var next int64
	s.mu.Lock()
	next = s.seq + 1
	s.mu.Unlock()
	if pending := s.Pending(); len(pending) > 0 {
		next = pending[0].Sequence
	}
	return Backlog{
		Pending:   len(s.Pending()),
		Committed: s.Cursor(),
		NextSeq:   next,
		Retained:  s.Len(),
	}
}
