package model

// SubscriptionState is the lifecycle state of a topic subscription node.
type SubscriptionState int

const (
	// SubActive means the node accepts new references.
	SubActive SubscriptionState = iota
	// SubDeleting means removal has started and new references are blocked.
	SubDeleting
	// SubRemoved means the node is gone from the tree.
	SubRemoved
)

// String renders the subscription state for the status endpoint.
func (s SubscriptionState) String() string {
	switch s {
	case SubActive:
		return "active"
	case SubDeleting:
		return "deleting"
	case SubRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

// SubscriptionView describes one subscriber binding for status output.
type SubscriptionView struct {
	Session   SessionID
	Topic     string
	State     SubscriptionState
	RefCount  int
	StateText string
}
