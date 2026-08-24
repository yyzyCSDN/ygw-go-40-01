package flow

import "fmt"

// Policy controls the token bucket used for one session.
type Policy struct {
	// Rate is the number of events allowed per second.
	Rate float64
	// Burst is the maximum burst size.
	Burst int
}

// Validate rejects nonsensical policies at startup.
func (p Policy) Validate() error {
	if p.Rate <= 0 {
		return fmt.Errorf("rate must be positive: %v", p.Rate)
	}
	if p.Burst <= 0 {
		return fmt.Errorf("burst must be positive: %d", p.Burst)
	}
	return nil
}
