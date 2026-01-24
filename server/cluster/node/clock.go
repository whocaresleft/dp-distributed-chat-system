package node

import "sync"

// LogicalClock has a counter protected by a Mutex.
// It offers functions that can used to implement Lamport Clocks
type LogicalClock struct {
	counter uint64
	mutex   sync.Mutex
}

// NewLogicalClock Creates and returns a new, empty, logical clock
func NewLogicalClock() *LogicalClock {
	return &LogicalClock{
		0, sync.Mutex{},
	}
}

// IncrementClock increments this node's logical clock and returns its value
func (l *LogicalClock) IncrementClock() uint64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.counter++
	return l.counter
}

// UpdateClock updates this node's logical clock based on the received one
func (l *LogicalClock) UpdateClock(received uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if received > l.counter {
		l.counter = received
	}
	l.counter++
}

// Snapshot returns the current value of the clock.
// Useful for reading without modifying
func (l *LogicalClock) Snapshot() uint64 {
	return l.counter
}
