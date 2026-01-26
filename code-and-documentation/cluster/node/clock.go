/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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
