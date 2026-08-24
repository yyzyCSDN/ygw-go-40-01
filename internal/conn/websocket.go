package conn

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket is a concurrency-safe wrapper around a raw websocket
// connection. All frame writes are serialized so control messages and
// payloads never interleave on the wire.
type WebSocket struct {
	raw    *websocket.Conn
	mu     sync.Mutex
	closed bool
}

// NewWebSocket wraps an upgraded raw connection.
func NewWebSocket(raw *websocket.Conn) *WebSocket {
	return &WebSocket{raw: raw}
}

// WriteMessage sends one frame. Writes after Close return ErrClosed.
func (w *WebSocket) WriteMessage(kind int, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.raw.WriteMessage(kind, data)
}

// ReadMessage reads the next frame from the client.
func (w *WebSocket) ReadMessage() (int, []byte, error) {
	return w.raw.ReadMessage()
}

// Close releases the raw connection exactly once.
func (w *WebSocket) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.raw.Close()
}

// RemoteAddr returns the peer address.
func (w *WebSocket) RemoteAddr() string {
	return w.raw.RemoteAddr().String()
}

// SetReadDeadline bounds the next read on the raw connection.
func (w *WebSocket) SetReadDeadline(t time.Time) error {
	return w.raw.SetReadDeadline(t)
}
