// Package conn adapts the transport used by the push gateway. The
// production implementation wraps gorilla/websocket so frames are
// serialized and the connection can be released exactly once.
package conn

import (
	"errors"
	"time"
)

// ErrClosed is returned when a frame targets an already released
// transport.
var ErrClosed = errors.New("connection is closed")

// Conn is the transport contract used by sessions and writers.
type Conn interface {
	// WriteMessage writes one complete message frame.
	WriteMessage(kind int, data []byte) error
	// ReadMessage reads the next message frame.
	ReadMessage() (kind int, data []byte, err error)
	// Close releases the underlying transport exactly once.
	Close() error
	// RemoteAddr returns the peer address for diagnostics.
	RemoteAddr() string
	// SetReadDeadline bounds the next read.
	SetReadDeadline(t time.Time) error
}
