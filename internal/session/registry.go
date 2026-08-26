package session

import (
	"sort"
	"sync"
	"time"

	"eventpush/internal/conn"
	"eventpush/internal/model"
)

// SessionRegistry holds the live sessions keyed by session id. A closed
// session is removed so a later bind with the same id starts a fresh
// incarnation with its own confirmation state.
type SessionRegistry struct {
	mu       sync.Mutex
	byID     map[model.SessionID]*Session
	active   map[*Session]struct{}
	lastSeen map[model.SessionID]time.Time
}

// NewSessionRegistry creates an empty registry.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		byID:     make(map[model.SessionID]*Session),
		active:   make(map[*Session]struct{}),
		lastSeen: make(map[model.SessionID]time.Time),
	}
}

// Bind returns the active session for an id or creates a new one.
func (r *SessionRegistry) Bind(id model.SessionID, c conn.Conn, cfg Config) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.byID[id]; ok {
		return s
	}
	s := New(id, c, cfg)
	r.byID[id] = s
	r.active[s] = struct{}{}
	r.lastSeen[id] = time.Now().UTC()
	return s
}

// Remove releases a session and its transport.
func (r *SessionRegistry) Remove(s *Session) {
	r.mu.Lock()
	delete(r.active, s)
	delete(r.lastSeen, s.id)
	r.mu.Unlock()
	s.FinishClose()
}

// Get returns the session for an id, or nil.
func (r *SessionRegistry) Get(id model.SessionID) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byID[id]
}

// Count returns the number of active sessions.
func (r *SessionRegistry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.active)
}

// IsLive reports whether a session id maps to a connection that is not
// closing or closed; renewal and delivery must not target anything else.
func (r *SessionRegistry) IsLive(id model.SessionID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.byID[id]
	if !ok {
		return false
	}
	st := s.State()
	return st != model.ConnClosing && st != model.ConnClosed
}

// Touch renews the heartbeat timestamp of a session. Renewal fails for
// sessions that are already closing or closed so an evicted connection
// can never be revived by a late heartbeat.
func (r *SessionRegistry) Touch(id model.SessionID, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.byID[id]
	if !ok {
		return false
	}
	if st := s.State(); st == model.ConnClosing || st == model.ConnClosed {
		return false
	}
	r.lastSeen[id] = now
	return true
}

// LastSeen returns the last heartbeat time of a session.
func (r *SessionRegistry) LastSeen(id model.SessionID) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	last, ok := r.lastSeen[id]
	return last, ok
}

// Sessions returns a snapshot of the active sessions.
func (r *SessionRegistry) Sessions() []*Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Session, 0, len(r.active))
	for s := range r.active {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// Snapshot returns the public view of every session for the status
// endpoint.
func (r *SessionRegistry) Snapshot() []model.SessionInfo {
	sessions := r.Sessions()
	out := make([]model.SessionInfo, 0, len(sessions))
	for _, s := range sessions {
		last, _ := r.LastSeen(s.ID())
		out = append(out, model.SessionInfo{
			ID:          s.ID(),
			State:       s.State(),
			Remote:      s.Conn().RemoteAddr(),
			Topics:      s.Topics(),
			LastSeen:    last,
			ReplayOpen:  s.InReplay(),
			AckedSeq:    s.Acked(),
			QueueDepth:  s.QueueDepth(),
			Pending:     s.Tracker().Len(),
			StateText:   s.State().String(),
			Credits:     s.Credits(),
		})
	}
	return out
}
