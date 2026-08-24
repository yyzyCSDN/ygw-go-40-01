package subscription

import (
	"sync"

	"eventpush/internal/model"
)

// RefCounter tracks how many times each session is bound to each topic.
// A session may subscribe to the same topic more than once; the counter
// keeps the binding consistent until every leave has been seen.
type RefCounter struct {
	mu      sync.RWMutex
	byTopic map[string]map[model.SessionID]int
}

// NewRefCounter creates an empty counter.
func NewRefCounter() *RefCounter {
	return &RefCounter{byTopic: make(map[string]map[model.SessionID]int)}
}

// Add increments the binding count for a session on a topic.
func (c *RefCounter) Add(topic string, sid model.SessionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byTopic[topic] == nil {
		c.byTopic[topic] = make(map[model.SessionID]int)
	}
	c.byTopic[topic][sid]++
}

// Sub decrements the binding count for a session on a topic.
func (c *RefCounter) Sub(topic string, sid model.SessionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byTopic[topic] == nil {
		return
	}
	if c.byTopic[topic][sid] <= 1 {
		delete(c.byTopic[topic], sid)
		return
	}
	c.byTopic[topic][sid]--
}

// Count returns how many times sid is bound to topic.
func (c *RefCounter) Count(topic string, sid model.SessionID) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.byTopic[topic][sid]
}

// Sum returns the total bindings for a topic across all sessions.
func (c *RefCounter) Sum(topic string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := 0
	for _, n := range c.byTopic[topic] {
		total += n
	}
	return total
}

// Clear drops every binding for a topic.
func (c *RefCounter) Clear(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.byTopic, topic)
}

// Topics returns the topics that still carry bindings.
func (c *RefCounter) Topics() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.byTopic))
	for topic := range c.byTopic {
		out = append(out, topic)
	}
	return out
}
