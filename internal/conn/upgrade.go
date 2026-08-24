package conn

import (
	"net/http"

	"github.com/gorilla/websocket"
)

// Upgrader wraps the gorilla websocket upgrader with the gateway's
// buffer policy.
type Upgrader struct {
	up websocket.Upgrader
}

// NewUpgrader builds an upgrader with the requested frame buffers.
func NewUpgrader(readBuf, writeBuf int) *Upgrader {
	return &Upgrader{
		up: websocket.Upgrader{
			ReadBufferSize:  readBuf,
			WriteBufferSize: writeBuf,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

// Upgrade performs the WebSocket handshake and wraps the raw connection
// in a transport-safe Conn.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (Conn, error) {
	raw, err := u.up.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return NewWebSocket(raw), nil
}
