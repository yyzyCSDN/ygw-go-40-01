package fanout

import "eventpush/internal/model"

// snapshotEntry pairs a subscriber snapshot with the topic version it
// was taken from.
type snapshotEntry struct {
	version  uint64
	sessions []model.SessionID
}

// CacheStats reports fanout snapshot cache behavior for diagnostics.
type CacheStats struct {
	Hits   uint64
	Misses uint64
}

// takeSnapshot returns the subscribers of a topic, reusing the cache
// only while the topic version is unchanged.
func (f *Fanout) takeSnapshot(topic string) []model.SessionID {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry, ok := f.cache[topic]; ok {
		return entry.sessions
	}
	sessions := f.subs.SnapshotBatch(topic)
	f.cache[topic] = snapshotEntry{version: f.subs.Version(topic), sessions: sessions}
	return sessions
}

// CacheStats returns the snapshot cache hit and miss counters.
func (f *Fanout) CacheStats() CacheStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return CacheStats{Hits: f.hits, Misses: f.misses}
}
