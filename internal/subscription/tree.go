package subscription

import (
	"strings"
	"sync"
)

// Node is one level of the topic tree. Every node keeps a reference
// count so deleting a topic can reclaim all of its subtree state.
type Node struct {
	children map[string]*Node
	refs     int
	deleted  bool
}

// Tree is the hierarchical topic index with a generation counter used
// to invalidate fanout snapshots.
type Tree struct {
	mu   sync.RWMutex
	root *Node
}

// NewTree creates an empty topic tree.
func NewTree() *Tree {
	return &Tree{root: &Node{children: make(map[string]*Node)}}
}

// Path splits a topic into its path segments.
func Path(topic string) []string {
	parts := strings.Split(topic, "/")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Join adds one reference along the topic path.
func (t *Tree) Join(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.root
	for _, part := range Path(topic) {
		if node.children[part] == nil {
			node.children[part] = &Node{children: make(map[string]*Node)}
		}
		node = node.children[part]
		node.refs++
	}
}

// Leave removes one reference along the topic path.
func (t *Tree) Leave(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.root
	for _, part := range Path(topic) {
		next := node.children[part]
		if next == nil {
			return
		}
		next.refs--
		node = next
	}
}

// Delete removes the topic subtree and clears every reference along
// the path so a later subscribe starts from a clean state.
func (t *Tree) Delete(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	parts := Path(topic)
	if len(parts) == 0 {
		return
	}
	t.removePath(t.root, parts, 0)
}

func (t *Tree) removePath(node *Node, parts []string, depth int) bool {
	part := parts[depth]
	child := node.children[part]
	if child == nil {
		return false
	}
	if depth == len(parts)-1 {
		child.refs = 0
		child.deleted = true
		delete(node.children, part)
		return true
	}
	removed := t.removePath(child, parts, depth+1)
	child.refs = 0
	if removed {
		child.deleted = true
		if len(child.children) == 0 && child.refs == 0 {
			delete(node.children, part)
		}
	}
	return removed
}
