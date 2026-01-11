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

// A Post election context contains the node's information after the last election process.
// This is a stable election context, valid until a new election process is performed.
// To read about the context of the current election process, go to `election_context.go, electionContext`.
// These information includes: The role of the node, the leader's ID and the ID's of the persistence nodes.
type PostElectionContext struct {
	role           node.NodeRole // Role of the node, specifies what are the responsibilities of a particular node in the system.
	leaderId       node.NodeId   // Id of the leader node.
	electionId     election_definitions.ElectionId
	persistenceIds map[node.NodeId]node.NodeId // Map of the persistence nodes. Maps each persistence node ID (leader included) to the ID of the next hop, that is, the neighboring node to follow to reach the destination.
}

// Creates a context with the specified role and leader ID
func NewPostElectionContext(role node.NodeRole, leader node.NodeId, electionId election_definitions.ElectionId) *PostElectionContext {
	return &PostElectionContext{role, leader, electionId, make(map[node.NodeId]node.NodeId)}
}

// Adds a pair (persistence node id, next hop node) to the node, if the persistence node id doesn't already have a next hop associated.
func (p *PostElectionContext) AddPersistence(persistence, nextHop node.NodeId) error {
	if p.DoesNextHopExists(persistence) {
		return fmt.Errorf("The ID %d corresponds to an already present node, use .Replace() to replace it", persistence)
	}
	p.persistenceIds[persistence] = nextHop
	return nil
}

// Replaces the next hop of a given persistence node, if such node already had a value (the pair existed).
func (p *PostElectionContext) ReplacePersistence(persistence, nextHop node.NodeId) error {
	if !p.DoesNextHopExists(persistence) {
		return fmt.Errorf("The ID %d does not correspond to any node, use .Add() to add it", persistence)
	}
	p.persistenceIds[persistence] = nextHop
	return nil
}

// Removes the next hop value of the given persistence node, if present.
func (p *PostElectionContext) RemovePersistence(persistence node.NodeId) error {
	if !p.DoesNextHopExists(persistence) {
		return fmt.Errorf("The ID %d does not correspond to any neighbor, can't proceed to deletion", persistence)
	}
	delete(p.persistenceIds, persistence)
	return nil
}

// Returns the next hop for the given persistence node, or an error if not present.
func (p *PostElectionContext) GetNextHop(persistence node.NodeId) (node.NodeId, error) {
	node, ok := p.persistenceIds[persistence]
	if !ok {
		return 0, fmt.Errorf("The ID %d does not correspond to any neighbor", persistence)
	}
	return node, nil
}

// Returns the leader node's ID.
func (p *PostElectionContext) GetLeader() node.NodeId {
	return p.leaderId
}

// Returns the node's role.
func (p *PostElectionContext) GetRole() node.NodeRole {
	return p.role
}

func (p *PostElectionContext) GetElectionId() election_definitions.ElectionId {
	return p.electionId
}

// Returns true if a next hop for the given persistence node is set.
func (p *PostElectionContext) DoesNextHopExists(persistence node.NodeId) bool {
	_, ok := p.persistenceIds[persistence]
	return ok
}

// Returns the number of persistence nodes known.
func (p *PostElectionContext) PersistenceNodeLength() int {
	return len(p.persistenceIds)
}
