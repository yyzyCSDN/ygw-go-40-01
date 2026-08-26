package heartbeat

import "time"

func (h *Heartbeat) stale(age time.Duration) bool {
	return age > h.timeout
}

func (h *Heartbeat) suspectAge(age time.Duration) bool {
	return age > h.suspect
}
