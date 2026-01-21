package node

import "sync"

type LogicalClock struct {
	counter uint64
	mutex   sync.Mutex
}

func NewLogicalClock() *LogicalClock {
	return &LogicalClock{
		0, sync.Mutex{},
	}
}

// Increments this node's logical clock and returns its value
func (l *LogicalClock) IncrementClock() uint64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.counter++
	return l.counter
}

// Updates this node's logical clock based on the received one
func (l *LogicalClock) UpdateClock(received uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if received > l.counter {
		l.counter = received
	}
	l.counter++
}

func (l *LogicalClock) Snapshot() uint64 {
	return l.counter
}
