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
	election_definitions "server/cluster/election/definitions"
	"server/cluster/node"
)

const RejoinLeaderTimeout = uint8(5)
const StartoverLeaderTimeout = uint8(10)

// A Post election context contains the node's information after the last election process.
// This is a stable election context, valid until a new election process is performed.
// To read about the context of the current election process, go to `election_context.go, electionContext`.
// These information includes: The role of the node, the leader's ID and the ID's of the persistence nodes.
type ElectionResult struct {
	leaderId   node.NodeId // Id of the leader node.
	electionId election_definitions.ElectionId
}

// Creates a context with the specified role and leader ID
func NewElectionResult(leader node.NodeId, electionId election_definitions.ElectionId) *ElectionResult {
	return &ElectionResult{
		leaderId:   leader,
		electionId: electionId,
	}
}

// Returns the leader node's ID.
func (e *ElectionResult) GetLeaderID() node.NodeId {
	return e.leaderId
}

// Returns the election's ID.
func (e *ElectionResult) GetElectionID() election_definitions.ElectionId {
	return e.electionId
}

func (e *ElectionResult) String() string {
	return fmt.Sprintf("Election Result{LeaderID{%d}, ElectionID{%s}}", e.leaderId, e.electionId)
}
