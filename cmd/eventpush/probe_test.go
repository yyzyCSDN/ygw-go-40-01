package main

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"eventpush"
	"eventpush/internal/event"
	"eventpush/internal/fanout"
	"eventpush/internal/heartbeat"
	"eventpush/internal/metric"
	"eventpush/internal/publish"
	"eventpush/internal/resume"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

func newTestServer() *Server {
	cfg := Load("127.0.0.1:0")
	store := event.NewStore(32)
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	dispatcher := fanout.New(topics, sessions, metrics)
	broker := publish.New(store, dispatcher, metrics)
	ingress := publish.NewIngress(broker, cfg.Publish)
	resumer := resume.New(store)
	evictor := heartbeat.NewEvictor(sessions, metrics)
	return NewServer(cfg, topics, sessions, metrics, ingress, broker, dispatcher, store, resumer, evictor)
}

func TestProbeEndpoints(t *testing.T) {
	srv := newTestServer()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	if err := Probe(ts.URL); err != nil {
		t.Fatalf("probe failed: %v", err)
	}
}

func TestConsoleMarker(t *testing.T) {
	if !strings.Contains(string(web.ConsoleHTML), "EventPush Console") {
		t.Fatal("console marker missing")
	}
	if !bytes.Contains(web.ConsoleHTML, []byte("WebSocket")) {
		t.Fatal("console page should reference WebSocket")
	}
}
