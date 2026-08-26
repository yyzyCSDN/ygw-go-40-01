package subscription

import (
	"sync"

	"eventpush/internal/model"
)

// Registry tracks which sessions are bound to which topics. It is the
// membership view used by the fanout dispatcher.
type Registry struct {
	mu        sync.RWMutex
	refs      *RefCounter
	byTopic   map[string]map[model.SessionID]struct{}
	bySession map[model.SessionID]map[string]struct{}
}

// NewRegistry creates an empty membership registry.
func NewRegistry() *Registry {
	return &Registry{
		refs:      NewRefCounter(),
		byTopic:   make(map[string]map[model.SessionID]struct{}),
		bySession: make(map[model.SessionID]map[string]struct{}),
	}
}

// Join binds a session to a topic.
func (r *Registry) Join(sid model.SessionID, topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs.Add(topic, sid)
	if r.byTopic[topic] == nil {
		r.byTopic[topic] = make(map[model.SessionID]struct{})
	}
	r.byTopic[topic][sid] = struct{}{}
	if r.bySession[sid] == nil {
		r.bySession[sid] = make(map[string]struct{})
	}
	r.bySession[sid][topic] = struct{}{}
}

// Leave unbinds a session from a topic when its last reference is gone.
func (r *Registry) Leave(sid model.SessionID, topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs.Sub(topic, sid)
	if r.refs.Count(topic, sid) == 0 {
		delete(r.byTopic[topic], sid)
		delete(r.bySession[sid], topic)
	}
}

// ClearTopic drops every binding for a topic, including its reference
// counts, so a later subscribe starts from zero.
func (r *Registry) ClearTopic(topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for sid := range r.byTopic[topic] {
		delete(r.bySession[sid], topic)
	}
	delete(r.byTopic, topic)
}

// Has reports whether a session is bound to a topic.
func (r *Registry) Has(sid model.SessionID, topic string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.byTopic[topic][sid]
	return ok
}

// Count returns the total bindings for a topic.
func (r *Registry) Count(topic string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.refs.Sum(topic)
}

// Subscribers returns the sessions bound to a topic.
func (r *Registry) Subscribers(topic string) []model.SessionID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]model.SessionID, 0, len(r.byTopic[topic]))
	for sid := range r.byTopic[topic] {
		out = append(out, sid)
	}
	return out
}

// Topics returns the topics that currently carry bindings.
func (r *Registry) Topics() []string {
	return r.refs.Topics()
}
