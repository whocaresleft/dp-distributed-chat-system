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
)

const RejoinLeaderTimeout = uint8(5)     //RejoinLeaderTimeout is the number of timeouts before probing the current leader
const StartoverLeaderTimeout = uint8(10) //StartoverLeaderTimeout is the number of timeouts before starting a new election

// ElectionResult contains the node's information after the last election process.
// This is a stable election context, valid until a new election process is performed.
// To read about the context of the current election process, go to `election_context.go, ElectionContext`.
// It stores the leader ID and the election ID
type ElectionResult struct {
	leaderId   node.NodeId // Id of the leader node
	electionId ElectionId  // Id of the election the leader won
}

// NewElectionResult creates a context with the specified leader and election IDs
func NewElectionResult(leader node.NodeId, electionId ElectionId) *ElectionResult {
	return &ElectionResult{
		leaderId:   leader,
		electionId: electionId,
	}
}

// GetLeaderID returns the leader node's ID.
func (e *ElectionResult) GetLeaderID() node.NodeId {
	return e.leaderId
}

// GetElectionID returns the election's ID.
func (e *ElectionResult) GetElectionID() ElectionId {
	return e.electionId
}

// String returns the string representation of an ElectionResult
func (e *ElectionResult) String() string {
	return fmt.Sprintf("Election Result{LeaderID{%d}, ElectionID{%s}}", e.leaderId, e.electionId)
}
