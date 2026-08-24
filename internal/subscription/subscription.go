package subscription

import (
	"sort"
	"sync"

	"eventpush/internal/model"
)

// TopicRegistry is the facade the rest of the gateway uses to manage
// subscriptions. It keeps the tree and the membership registry in
// sync, and publishes a per-topic version that fanout uses to decide
// whether a cached snapshot is still valid.
type TopicRegistry struct {
	reg      *Registry
	tree     *Tree
	versions map[string]uint64
	mu       sync.Mutex
}

// NewTopicRegistry builds the subscription facade.
func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{
		reg:      NewRegistry(),
		tree:     NewTree(),
		versions: make(map[string]uint64),
	}
}

// Subscribe binds a session to a topic.
func (t *TopicRegistry) Subscribe(sid model.SessionID, topic string) {
	t.tree.Join(topic)
	t.reg.Join(sid, topic)
	t.bump(topic)
}

// Unsubscribe removes one binding of a session from a topic.
func (t *TopicRegistry) Unsubscribe(sid model.SessionID, topic string) {
	t.tree.Leave(topic)
	t.reg.Leave(sid, topic)
	t.bump(topic)
}

// DeleteTopic removes the topic from the tree and the membership
// registry, and bumps its version so cached fanout snapshots are
// discarded.
func (t *TopicRegistry) DeleteTopic(topic string) {
	t.tree.Delete(topic)
	t.reg.ClearTopic(topic)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.versions[topic]; ok {
		t.versions[topic]++
		delete(t.versions, topic)
	}
}

// Subscribers returns the sessions currently bound to a topic.
func (t *TopicRegistry) Subscribers(topic string) []model.SessionID {
	return t.reg.Subscribers(topic)
}

// SnapshotBatch returns a stable, sorted copy of the subscribers for a
// topic. Fanout uses it once per batch so a subscription that joins in
// the middle of the batch cannot receive a partial batch.
func (t *TopicRegistry) SnapshotBatch(topic string) []model.SessionID {
	sessions := t.reg.Subscribers(topic)
	sort.Slice(sessions, func(i, j int) bool { return sessions[i] < sessions[j] })
	return sessions
}

// Has reports whether a session is bound to a topic.
func (t *TopicRegistry) Has(sid model.SessionID, topic string) bool {
	return t.reg.Has(sid, topic)
}

// Count returns the total bindings for a topic.
func (t *TopicRegistry) Count(topic string) int {
	return t.reg.Count(topic)
}

// Version returns the current version for a topic; versions only grow
// and change whenever the topic's membership changes.
func (t *TopicRegistry) Version(topic string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.versions[topic]
}

// Snapshot returns the bindings of a topic for the status endpoint.
func (t *TopicRegistry) Snapshot(topic string) []model.SubscriptionView {
	sessions := t.Subscribers(topic)
	out := make([]model.SubscriptionView, 0, len(sessions))
	for _, sid := range sessions {
		out = append(out, model.SubscriptionView{
			Session:   sid,
			Topic:     topic,
			State:     model.SubActive,
			RefCount:  t.Count(topic),
			StateText: model.SubActive.String(),
		})
	}
	return out
}

// AllTopics returns every topic that currently has subscribers.
func (t *TopicRegistry) AllTopics() []string {
	topics := t.reg.Topics()
	sort.Strings(topics)
	return topics
}

// SnapshotAll returns the subscription view for every topic.
func (t *TopicRegistry) SnapshotAll() []model.SubscriptionView {
	var out []model.SubscriptionView
	for _, topic := range t.AllTopics() {
		out = append(out, t.Snapshot(topic)...)
	}
	return out
}

func (t *TopicRegistry) bump(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.versions[topic]++
}
