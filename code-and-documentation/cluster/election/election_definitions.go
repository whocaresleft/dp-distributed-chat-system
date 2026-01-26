/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package election

import (
	"fmt"
	"server/cluster/node"
	"strconv"
	"strings"
	"time"
)

// Time of a timeout regarding the election process
const Timeout = time.Second * 30

// LinkDirection represents how an oriented acr is oriented.
type LinkDirection bool

// Suppose arc nodes i and j.
const (
	Outgoing LinkDirection = true  // (i, j)
	Incoming LinkDirection = false // (j, i)
)

// ElectionId identifies uniquely an election. Here it's calculated using ID-TS.
// Where ID is the ID of the node that started the election, and TS the current timestamp of the logical clock.
type ElectionId string

const InvalidId ElectionId = ""

// Compare compares two election ids to determine which is stronger
// It follows Java's compare to: e.CompareTo(other):
//
// <0 implies e < other
//
//	0 implies e = other
//
// >0 implies e > other
//
// The rule is, considering <node-id>-<clock>:
//
//	The bigger clock makrs the stronger election
//	If the clocks are the same, we consider the smaller node-id to be stronger
func (e ElectionId) Compare(other ElectionId) int64 {
	if e == other {
		return 0
	}
	if e == InvalidId {
		return -1
	}
	if other == InvalidId {
		return 1
	}

	clock1, discriminant1, _ := e.Parse()
	clock2, discriminant2, _ := other.Parse()

	if clock1 == clock2 {
		return int64(discriminant2) - int64(discriminant1)
	}
	return int64(clock1 - clock2)
}

// Helper function that parses an election ID
// It returns it's internal node-id and clock. When successful the error is nil
func (e ElectionId) Parse() (uint64, node.NodeId, error) {

	clockString, discriminantString, ok := strings.Cut(string(e), "-")
	if !ok {
		return 0, 0, fmt.Errorf("Id was mal-formatted")
	}

	clock, err := strconv.ParseInt(clockString, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	discriminant, err := strconv.ParseInt(discriminantString, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return uint64(clock), node.NodeId(discriminant), nil
}

// Inverse returns the orientation opposite of the one it was called on.
// That means:
//
//	l = Incoming, l.Inverse() = Outgoing
//	l = Outgoing, l.Inverse() = Incoming
func (l LinkDirection) Inverse() LinkDirection {
	return !l
}

////////////////////////////////////////////////

// ElectionStatus represents the role that a node has during an election phase
type ElectionStatus uint8

const (
	Source       ElectionStatus = iota // Node with only outgoing arcs
	InternalNode                       // Node with both incoming and outgoing arcs
	Sink                               // Node with only incoming arcs
	Loser                              // Node that lost the election and does not partecipate anymore
	Winner                             // Node that won the election
)

// Readable representation of each status
var readableStatuses = []string{
	"Source",
	"InternalNode",
	"Sink",
	"Loser",
	"Winner",
}

// String returns the readable representation of the status s
func (s ElectionStatus) String() string {
	return readableStatuses[uint(s)]
}

////////////////////////////////////////////////

// ElectionState represents the state of a node during the election round
type ElectionState uint8

const (
	Idle          ElectionState = iota // Either no current election or the current one is already lost
	WaitingYoDown                      // Wating for incoming links' proposals
	WaitingYoUp                        // Waiting for outgoing links' votes
)

// Readable representation of each state
var readableStates = []string{
	"Idle",
	"Waiting Yo Down",
	"Waiting Yo Up",
}

// String returns the readable representation of the state s
func (s ElectionState) String() string {
	return readableStates[uint(s)]
}
