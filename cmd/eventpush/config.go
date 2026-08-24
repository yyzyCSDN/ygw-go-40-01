// Command eventpush runs the real-time event push gateway with a
// WebSocket endpoint, a publish API and an operator console.
package main

import (
	"time"

	"eventpush/internal/publish"
	"eventpush/internal/session"
)

// Config carries the gateway runtime settings.
type Config struct {
	Addr              string
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	SuspectAfter      time.Duration
	Session           session.Config
	StoreCapacity     int
	ReadBuf           int
	WriteBuf          int
	Publish           publish.Config
}

// Load returns the default configuration for an address.
func Load(addr string) Config {
	return Config{
		Addr:              addr,
		HeartbeatInterval: 2 * time.Second,
		HeartbeatTimeout:  15 * time.Second,
		SuspectAfter:      8 * time.Second,
		Session:           session.DefaultConfig(),
		StoreCapacity:     2048,
		ReadBuf:           4096,
		WriteBuf:          4096,
		Publish:           publish.DefaultConfig(),
	}
}
