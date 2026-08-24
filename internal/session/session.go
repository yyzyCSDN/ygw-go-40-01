// Package session manages per-client connection state, the bounded
// write queue and the replay window used after a reconnect.
package session

import (
	"sync"
	"time"

	"eventpush/internal/ack"
	"eventpush/internal/conn"
	"eventpush/internal/flow"
	"eventpush/internal/model"
)

// Config carries the transport and flow settings for one session.
type Config struct {
	QueueSize    int
	WriteTimeout time.Duration
	Rate         float64
	Burst        int
}

// DefaultConfig returns sensible defaults for a demo deployment.
func DefaultConfig() Config {
	return Config{
		QueueSize:    64,
		WriteTimeout: 2 * time.Second,
		Rate:         200,
		Burst:        400,
	}
}

// Session is the server-side view of one client connection.
type Session struct {
	mu        sync.Mutex
	id        model.SessionID
	state     model.ConnState
	conn      conn.Conn
	writer    *Writer
	tracker   *ack.Tracker
	flow      *flow.Limiter
	cfg       Config
	epoch     uint64
	topics    map[string]struct{}
	replaying bool
	buffer    []model.Event
	closedCh  chan struct{}
}

// New creates a session bound to a transport connection.
func New(id model.SessionID, c conn.Conn, cfg Config) *Session {
	s := &Session{
		id:       id,
		state:    model.ConnActive,
		conn:     c,
		writer:   NewWriter(c, cfg.QueueSize, cfg.WriteTimeout),
		tracker:  ack.NewTracker(id),
		flow:     flow.NewLimiter(flow.Policy{Rate: cfg.Rate, Burst: cfg.Burst}),
		cfg:      cfg,
		topics:   make(map[string]struct{}),
		closedCh: make(chan struct{}),
	}
	s.epoch = s.tracker.Epoch()
	return s
}

// ID returns the session identifier.
func (s *Session) ID() model.SessionID {
	return s.id
}

// State returns the current connection state.
func (s *Session) State() model.ConnState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Conn returns the underlying transport.
func (s *Session) Conn() conn.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// Writer returns the session write queue.
func (s *Session) Writer() *Writer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writer
}

// Tracker returns the confirmation tracker of the session.
func (s *Session) Tracker() *ack.Tracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tracker
}

// Reconnect attaches a new transport to the session. Confirmation state
// is reset so the delivery position starts over from a clean window.
func (s *Session) Reconnect(c conn.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = c
	s.writer = NewWriter(c, s.cfg.QueueSize, s.cfg.WriteTimeout)
	s.tracker.Reset()
	s.epoch = s.tracker.Epoch()
	s.flow = flow.NewLimiter(flow.Policy{Rate: s.cfg.Rate, Burst: s.cfg.Burst})
	s.state = model.ConnActive
	s.replaying = false
	s.buffer = nil
	s.closedCh = make(chan struct{})
}

// BeginClose transitions active or suspected connections to closing.
func (s *Session) BeginClose() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case model.ConnActive, model.ConnSuspected:
		s.state = model.ConnClosing
		return true
	default:
		return false
	}
}

// Suspect marks an active connection as suspected after a missed
// heartbeat.
func (s *Session) Suspect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == model.ConnActive {
		s.state = model.ConnSuspected
	}
}

// FinishClose completes the closing transition and releases the
// writer's transport.
func (s *Session) FinishClose() {
	s.mu.Lock()
	s.state = model.ConnClosed
	s.replaying = false
	s.buffer = nil
	s.mu.Unlock()
	s.writer.Shutdown()
	close(s.closedCh)
}

// Deliver routes one live event through the ordered delivery path. The
// event is buffered while a replay window is open and recorded as
// delivered only when the writer accepted it.
func (s *Session) Deliver(ev model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == model.ConnClosing || s.state == model.ConnClosed {
		return ErrClosed
	}
	if s.tracker.Owner() != s.id || s.tracker.Epoch() != s.epoch {
		// A tracker from an older session incarnation must never leak
		// into this one; rebind a fresh tracker instead.
		s.tracker = ack.NewTracker(s.id)
		s.epoch = s.tracker.Epoch()
	}
	if s.tracker.Covered(ev.Sequence) || s.tracker.Pending(ev.Sequence) {
		return nil
	}
	if !s.flow.Allow(s.id) {
		return flow.ErrLimited
	}
	if s.replaying {
		s.buffer = append(s.buffer, ev)
		return nil
	}
	err := s.writer.Enqueue(ev)
	if err == nil {
		s.tracker.Record(ev.Sequence, true)
	}
	return err
}

// Confirm records a client confirmation for a sequence.
func (s *Session) Confirm(seq int64) {
	s.tracker.Confirm(seq)
}

// Acked returns the highest contiguous confirmed sequence.
func (s *Session) Acked() int64 {
	return s.tracker.Snapshot()
}

// Credits returns the remaining send budget of the session.
func (s *Session) Credits() float64 {
	return s.flow.Credits(s.id)
}

// ResumeFrom returns the first sequence a reconnect should replay.
func (s *Session) ResumeFrom() int64 {
	return s.Acked() + 1
}

// BeginReplay opens the ordered replay window. Live deliveries are
// buffered until EndReplay so replay events always arrive first.
func (s *Session) BeginReplay() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == model.ConnClosing || s.state == model.ConnClosed {
		return false
	}
	if s.replaying {
		return false
	}
	s.replaying = true
	s.buffer = nil
	return true
}

// ReplayWrite enqueues one replayed event into the writer queue.
func (s *Session) ReplayWrite(ev model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == model.ConnClosing || s.state == model.ConnClosed {
		return ErrClosed
	}
	return s.writer.Enqueue(ev)
}

// EndReplay closes the replay window and flushes buffered live events
// behind the replay events.
func (s *Session) EndReplay() {
	s.mu.Lock()
	flush := s.buffer
	s.replaying = false
	s.buffer = nil
	s.mu.Unlock()
	for _, ev := range flush {
		_ = s.writer.Enqueue(ev)
	}
}

// InReplay reports whether the session replay window is open.
func (s *Session) InReplay() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replaying
}

// Subscribe records a local topic binding for the session.
func (s *Session) Subscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.topics == nil {
		s.topics = make(map[string]struct{})
	}
	s.topics[topic] = struct{}{}
}

// Unsubscribe removes a local topic binding.
func (s *Session) Unsubscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.topics, topic)
}

// Topics returns the session's topic bindings.
func (s *Session) Topics() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.topics))
	for topic := range s.topics {
		out = append(out, topic)
	}
	return out
}

// QueueDepth returns the current writer queue occupancy.
func (s *Session) QueueDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writer.QueueDepth()
}
