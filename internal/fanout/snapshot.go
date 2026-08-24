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
	version := f.subs.Version(topic)
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry, ok := f.cache[topic]; ok {
		if entry.version == version {
			f.hits++
			return entry.sessions
		}
		f.dropTopic(topic)
	}
	f.misses++
	sessions := f.subs.SnapshotBatch(topic)
	f.cache[topic] = snapshotEntry{version: version, sessions: sessions}
	return sessions
}

// dropTopic removes one cached topic snapshot.
func (f *Fanout) dropTopic(topic string) {
	delete(f.cache, topic)
}

// CacheStats returns the snapshot cache hit and miss counters.
func (f *Fanout) CacheStats() CacheStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return CacheStats{Hits: f.hits, Misses: f.misses}
}
