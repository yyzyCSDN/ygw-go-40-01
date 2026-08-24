package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"eventpush/internal/conn"
	"eventpush/internal/event"
	"eventpush/internal/fanout"
	"eventpush/internal/heartbeat"
	"eventpush/internal/metric"
	"eventpush/internal/model"
	"eventpush/internal/publish"
	"eventpush/internal/resume"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

// Server wires the HTTP endpoints, WebSocket upgrade path and publish
// API together.
type Server struct {
	cfg      Config
	upgrader *conn.Upgrader
	topics   *subscription.TopicRegistry
	sessions *session.SessionRegistry
	metrics  *metric.Metrics
	ingress  *publish.Ingress
	broker   *publish.Broker
	fanoutD  *fanout.Fanout
	store    *event.Store
	resumer  *resume.Resumer
	evictor  *heartbeat.Evictor
	httpSrv  *http.Server
}

// NewServer builds the gateway server.
func NewServer(
	cfg Config,
	topics *subscription.TopicRegistry,
	sessions *session.SessionRegistry,
	metrics *metric.Metrics,
	ingress *publish.Ingress,
	broker *publish.Broker,
	fanoutD *fanout.Fanout,
	store *event.Store,
	resumer *resume.Resumer,
	evictor *heartbeat.Evictor,
) *Server {
	return &Server{
		cfg:      cfg,
		upgrader: conn.NewUpgrader(cfg.ReadBuf, cfg.WriteBuf),
		topics:   topics,
		sessions: sessions,
		metrics:  metrics,
		ingress:  ingress,
		broker:   broker,
		fanoutD:  fanoutD,
		store:    store,
		resumer:  resumer,
		evictor:  evictor,
	}
}

// Handler returns the HTTP routing table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/publish", s.handlePublish)
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/", s.handleConsole)
	return mux
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(addr string) error {
	s.httpSrv = &http.Server{Addr: addr, Handler: s.Handler()}
	return s.httpSrv.ListenAndServe()
}

// Shutdown stops the HTTP server gracefully.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"sessions":      s.sessions.Snapshot(),
		"session_count": s.sessions.Count(),
		"metrics":       s.metrics.Snapshot(),
		"backlog":       event.BacklogInfo(s.store),
		"fanout":        s.fanoutD.CacheStats(),
		"progress":      s.broker.Progress(),
		"subscriptions": s.topics.SnapshotAll(),
	})
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	seq, err := s.ingress.Handle(r.Context(), req.Topic, []byte(req.Payload))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{"sequence": seq, "status": "accepted"})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := s.upgrader.Upgrade(w, r)
	if err != nil {
		return
	}
	sid := model.SessionID(r.URL.Query().Get("session"))
	if sid == "" {
		sid = model.SessionID(newSessionToken())
	}
	sess := s.sessions.Get(sid)
	if sess != nil && sess.State() != model.ConnClosed {
		sess.Reconnect(c)
	} else {
		sess = s.sessions.Bind(sid, c, s.cfg.Session)
	}
	for _, topic := range splitTopics(r.URL.Query().Get("topics")) {
		s.topics.Subscribe(sid, topic)
		sess.Subscribe(topic)
	}
	go s.readLoop(sess)
	if s.resumer != nil {
		go func() {
			if err := s.resumer.Replay(sess); err != nil && err != resume.ErrAlreadyReplaying {
				log.Printf("replay for %s failed: %v", sid, err)
			}
		}()
	}
}

func (s *Server) readLoop(sess *session.Session) {
	c := sess.Conn()
	defer func() {
		if sess.Conn() == c {
			s.sessions.Remove(sess)
		}
	}()
	for {
		_ = c.SetReadDeadline(time.Now().Add(s.cfg.Session.WriteTimeout))
		_, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		s.applyFrame(sess, data)
	}
}

func (s *Server) applyFrame(sess *session.Session, data []byte) {
	fields := strings.Fields(strings.TrimSpace(string(data)))
	if len(fields) == 0 {
		return
	}
	switch fields[0] {
	case "ack":
		if len(fields) == 2 {
			if seq, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				sess.Confirm(seq)
				s.metrics.Acked()
			}
		}
	case "sub":
		if len(fields) == 2 {
			if !s.topics.Has(sess.ID(), fields[1]) {
				s.topics.Subscribe(sess.ID(), fields[1])
				sess.Subscribe(fields[1])
			}
		}
	case "unsub":
		if len(fields) == 2 {
			s.topics.Unsubscribe(sess.ID(), fields[1])
			sess.Unsubscribe(fields[1])
		}
	case "del":
		if len(fields) == 2 {
			s.topics.DeleteTopic(fields[1])
		}
	case "touch":
		_ = s.sessions.Touch(sess.ID(), time.Now().UTC())
	case "close":
		s.evictor.Evict(sess)
	}
}

func splitTopics(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func newSessionToken() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(buf[:])
}
