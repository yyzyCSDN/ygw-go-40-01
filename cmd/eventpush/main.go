package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"eventpush/internal/event"
	"eventpush/internal/fanout"
	"eventpush/internal/heartbeat"
	"eventpush/internal/metric"
	"eventpush/internal/publish"
	"eventpush/internal/resume"
	"eventpush/internal/session"
	"eventpush/internal/subscription"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	cfg := Load(*addr)
	store := event.NewStore(cfg.StoreCapacity)
	topics := subscription.NewTopicRegistry()
	sessions := session.NewSessionRegistry()
	metrics := metric.New()
	dispatcher := fanout.New(topics, sessions, metrics)
	broker := publish.New(store, dispatcher, metrics)
	ingress := publish.NewIngress(broker, cfg.Publish)
	resumer := resume.New(store)
	heart := heartbeat.New(sessions, metrics, cfg.HeartbeatInterval, cfg.HeartbeatTimeout, cfg.SuspectAfter)
	srv := NewServer(cfg, topics, sessions, metrics, ingress, broker, dispatcher, store, resumer, heart.Evictor())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go heart.Run(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(cfg.Addr)
	}()

	if err := probeWithRetry("http://"+cfg.Addr, 20); err != nil {
		log.Printf("startup probe failed: %v", err)
	} else {
		log.Printf("gateway listening on %s", cfg.Addr)
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown failed: %v", err)
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}
}
