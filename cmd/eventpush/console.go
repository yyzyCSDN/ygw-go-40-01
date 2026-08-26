package main

import (
	"net/http"

	"eventpush"
)

// handleConsole serves the embedded operator console page.
func (s *Server) handleConsole(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(web.ConsoleHTML)
}
