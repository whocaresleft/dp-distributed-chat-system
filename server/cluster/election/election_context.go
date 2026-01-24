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

// toleratedTimeouts is the number of tolerated timeouts before actually considering a node turned OFF.
const toleratedTimeouts = uint8(10)

// roundContext stores the necessary information for completing ONE election round
// Such information includes: smallest ID seen, the proposals, the votes, and the list of links to be pruned
type roundContext struct {
	epoch uint // Round number inside the election

	smallestId    node.NodeId // Smallest ID seen (updated each proposal. Starts with uint64MAX, each proposal, if less, replaces it)
	forwardedVote bool        // What do I vote? (updated each vote. Starts from yes and the first no sets it to no)

	// YO- Information
	awaitedProposals  uint                        // Number of awaited proposals
	receivedProposals map[node.NodeId]node.NodeId // Maps each incoming link's node to the id it proposed

	// -YO Information
	awaitedVotes  uint                 // Number of awaited votes
	receivedVotes map[node.NodeId]bool // Maps each outgoing link's node to the vote it sent

	pruneList []node.NodeId // List of nodes whose links will be pruned at the end of this round
}

// futureRoundContext stores informations that could have been received too early (e.g. a node is faster than this)
// It stores those informations that can be applied later, when we reach that epoch
type futureRoundContext struct {
	epoch             uint                        // Future round number
	receivedVotes     map[node.NodeId]bool        // Map of received votes (If when processing an earlier round a node gets pruned, we also remove its vote)
	receivedProposals map[node.NodeId]node.NodeId // Map of received proposals
}

// NewFutureRoundContext creates and returns a future round context for the given epoch
func NewFutureRoundContext(epoch uint) *futureRoundContext {
	return &futureRoundContext{
		epoch:             epoch,
		receivedProposals: make(map[node.NodeId]node.NodeId),
		receivedVotes:     make(map[node.NodeId]bool),
	}
}

// NewRoundContext creates and returns a Round context for the given round epoch
func NewRoundContext(epoch uint) *roundContext {
	return &roundContext{
		epoch:             epoch,
		smallestId:        node.NodeId(^uint64(0)),
		forwardedVote:     true,
		awaitedProposals:  0,
		receivedProposals: make(map[node.NodeId]node.NodeId),
		awaitedVotes:      0,
		receivedVotes:     make(map[node.NodeId]bool),
		pruneList:         make([]node.NodeId, 0),
	}
}

// setupNewRound resets this structure to allow a new round to take place
// epoch is the number of the entering round (the one that is being prepared)
func (r *roundContext) setupNewRound(epoch uint) {
	r.epoch = epoch
	r.smallestId = node.NodeId(^uint64(0))
	r.forwardedVote = true
	r.awaitedProposals = 0
	clear(r.receivedProposals)
	r.awaitedVotes = 0
	clear(r.receivedVotes)
	r.pruneList = r.pruneList[:0]
}

// ElectionContext contains all the necessary information to partecipate and complete an election process.
// This is NOT a stable context, as it is continuosly changed. An 'ElectionResult' is used instead after an election.
// To read about the context of the stable election result context, go to `election_result.go, ElectionResult`.
// It stores the leader ID and the election ID
type ElectionContext struct {
	idDiscriminant node.NodeId // Id of the node. Used to generate election ID
	idProposal     ElectionId  // ElectionID this node generated and proposed (to start an election)
	actualId       ElectionId  // ElectionID that 'won'. Every node must follow said ID

	receivedStart  bool // Did I receive the election start?
	receivedLeader bool // Did I receive the election end (leader message)?

	status           ElectionStatus                // Current status (Source, Internal, Sink, Loser, Winner)
	state            ElectionState                 // Current state (Idle, Waiting Yo Down, Waiting Yo Up)
	currentRound     *roundContext                 // Current round context
	futureRounds     map[uint]*futureRoundContext  // Map of possible future rounds, if ahead messages were received
	linksOrientation map[node.NodeId]LinkDirection // Orientation of each link with each neighbor
	orientationCount map[LinkDirection]uint        // Number of links for each orientation (user for simplicity, kept consistent manually after every change to linksOrientation)

	timeoutsCount map[node.NodeId]uint8 // Maps each current neighbor with a number of timeous.
}

// NewElectionContext creates and returns an election context
func NewElectionContext(discriminant node.NodeId) *ElectionContext {
	orientationCount := make(map[LinkDirection]uint)
	orientationCount[Incoming] = 0
	orientationCount[Outgoing] = 0
	return &ElectionContext{
		discriminant,
		InvalidId,
		InvalidId,
		false,
		false,
		Source,
		Idle,
		NewRoundContext(0),
		make(map[uint]*futureRoundContext),
		make(map[node.NodeId]LinkDirection),
		orientationCount,
		make(map[node.NodeId]uint8),
	}
}

// SetId sets the definitive election ID
func (e *ElectionContext) SetId(id ElectionId) {
	e.actualId = id
}

// GetId returns the definitive election ID
func (e *ElectionContext) GetId() ElectionId {
	return e.actualId
}

// GetId returns the proposed election ID (generated by this nome)
func (e *ElectionContext) GetIdProposal() ElectionId {
	return e.idProposal
}

// GenerateId returns an election ID generated using the node id and a clock timestamp. <clock>-<discriminant>
func GenerateId(clock uint64, discriminant node.NodeId) ElectionId {
	return ElectionId(fmt.Sprintf("%d-%d", clock, discriminant))
}

// generateId returns an election ID generated using this node's id and a clock timestamp. <clock>-<e.discriminant>
func (e *ElectionContext) generateId(clock uint64) ElectionId {
	return GenerateId(clock, e.idDiscriminant)
}

// Clear empties the election context, resetting it for a new round
func (e *ElectionContext) Clear() {
	e.idProposal = InvalidId
	e.idProposal = InvalidId
	e.receivedStart = false
	e.receivedLeader = false
	e.status = Source
	e.state = Idle
	e.orientationCount[Incoming] = 0
	e.orientationCount[Outgoing] = 0
	clear(e.linksOrientation)
	clear(e.timeoutsCount)
}

// setupNewRound prepares the epoch and number of awaited proposal and votes for the next round
func (e *ElectionContext) setupNewRound(epoch uint) {
	e.currentRound.setupNewRound(epoch)
	e.currentRound.awaitedProposals = e.InNodesCount()
	e.currentRound.awaitedVotes = e.OutNodesCount()
}

// Reset is an easier way to prepare a new election
// It clears the context, generated a proposed election ID and setups the first round
func (e *ElectionContext) Reset(clock uint64) {
	e.Clear()
	e.actualId = e.generateId(clock)
	e.FirstRound()
}

// GetAwaitedProposals return the number of awaited proposals
func (e *ElectionContext) GetAwaitedProposals() uint {
	return e.currentRound.awaitedProposals
}

// GetAwaitedVotes return the number of awaited votes
func (e *ElectionContext) GetAwaitedVotes() uint {
	return e.currentRound.awaitedVotes
}

// GetAllProposals return all the proposals received
func (e *ElectionContext) GetAllProposals() map[node.NodeId]node.NodeId {
	return e.currentRound.receivedProposals
}

// GetAllVotes return all the vtoes received
func (e *ElectionContext) GetAllVotes() map[node.NodeId]bool {
	return e.currentRound.receivedVotes
}

// PruneThisRound marks the node, id, to be pruned after the end of this round
func (e *ElectionContext) PruneThisRound(id node.NodeId) {
	e.currentRound.pruneList = append(e.currentRound.pruneList, id)
}

// FirstRound is used to setup the first round of the election
// Internally it does setupNewRound(0)
func (e *ElectionContext) FirstRound() {
	e.setupNewRound(0)
}

// prune removes the nodes marked for pruning
func (e *ElectionContext) prune() {
	for _, id := range e.currentRound.pruneList {
		e.Remove(id)
	}
}

// NextRound prepares for the next election round
// It updates and prunes the links, updates the status and processes any future vote and proposal
func (e *ElectionContext) NextRound() {
	e.updateLinks()
	e.prune()
	e.UpdateStatus()

	nextRound := e.CurrentRound() + 1
	e.setupNewRound(nextRound)

	futureContext, ok := e.futureRounds[nextRound]
	if !ok {
		return
	}

	for sender, futureProposal := range futureContext.receivedProposals {
		e.StoreProposal(sender, futureProposal)
	}
	for sender, futureVote := range futureContext.receivedVotes {
		e.StoreVote(sender, futureVote)
	}

	delete(e.futureRounds, nextRound)
}

// ResetTimeouts resets the accumulated timeouts of the given node
func (e *ElectionContext) ResetTimeouts(node node.NodeId) error {
	_, ok := e.timeoutsCount[node]
	if !ok {
		return fmt.Errorf("Node %d was not registered", node)
	}
	e.timeoutsCount[node] = 0
	return nil
}

// IsFaulty checks if the given node has a number of timeouts that is greater or equal than the tolerated amount
// Is it is successful, error is nil
func (e *ElectionContext) IsFaulty(node node.NodeId) (bool, error) {
	timeouts, err := e.GetTimeout(node)
	if err != nil {
		return false, err
	}
	return timeouts >= toleratedTimeouts, nil
}

// HasReceivedStart tells whether the start message was received
func (e *ElectionContext) HasReceivedStart() bool {
	return e.receivedStart
}

// HasReceivedLeader tells whether the end (leader) message was received
func (e *ElectionContext) HasReceivedLeader() bool {
	return e.receivedLeader
}

// SetStartReceived marks the start message as received
func (e *ElectionContext) SetStartReceived() {
	e.receivedStart = true
}

// SetLeaderReceived marks the end (leader) message as received
func (e *ElectionContext) SetLeaderReceived() {
	e.receivedLeader = true
}

// CurrentRound returns the current round number
func (e *ElectionContext) CurrentRound() uint {
	return e.currentRound.epoch
}

// Exists tells whether a link towards id exits
func (e *ElectionContext) Exists(id node.NodeId) bool {
	_, ok := e.linksOrientation[id]
	return ok
}

// Add adds the node, with identifier id, to the current election neighbors,
// creating the link with the given orientation
func (e *ElectionContext) Add(id node.NodeId, orientation LinkDirection) error {
	if e.Exists(id) {
		return fmt.Errorf("Node %d is already present", id)
	}
	e.linksOrientation[id] = orientation
	e.orientationCount[orientation]++
	e.timeoutsCount[id] = 0
	return nil
}

// Remove removes the node, with identifier id, from the current election neighbors,
// removing the link as well as the timeouts
func (e *ElectionContext) Remove(id node.NodeId) error {
	if !e.Exists(id) {
		return fmt.Errorf("Node %d wasn't present", id)
	}
	orientation := e.linksOrientation[id]
	delete(e.linksOrientation, id)
	delete(e.timeoutsCount, id)
	e.orientationCount[orientation]--
	return nil
}

// SetOrientation sets the orientation of the link with node id.
// If successful, the error is nil
func (e *ElectionContext) SetOrientation(id node.NodeId, orientation LinkDirection) error {
	if !e.Exists(id) {
		return fmt.Errorf("Node %d is not present, you can add it with .Add()", id)
	}
	oldOrientation := e.linksOrientation[id]
	e.linksOrientation[id] = orientation
	e.orientationCount[oldOrientation]--
	e.orientationCount[orientation]++
	return nil
}

// GetOrientation returns the orientation of the link with node id.
// If successful, the error is nil
func (e *ElectionContext) GetOrientation(id node.NodeId) (LinkDirection, error) {
	if !e.Exists(id) {
		return false, fmt.Errorf("Node %d is not present, you can add it with .Add()", id)
	}
	return e.linksOrientation[id], nil
}

// IncreaseTimeout increases by 1 the timeouts accumulated by the node id.
// If successful, the error is nil
func (e *ElectionContext) IncreaseTimeout(id node.NodeId) error {
	if !e.Exists(id) && id != e.idDiscriminant {
		return fmt.Errorf("Node %d is not present, you can add it with .Add() first", id)
	}
	e.timeoutsCount[id]++
	return nil
}

// GetTimeout returns the timeouts accumulated by the node id.
// If successful, the error is nil
func (e *ElectionContext) GetTimeout(id node.NodeId) (uint8, error) {
	if !e.Exists(id) && id != e.idDiscriminant {
		return 0, fmt.Errorf("Node %d is not present, you can add it with .Add() first", id)
	}
	return e.timeoutsCount[id], nil
}

// UpdateStatus calculates the status of the node, based on the current links
//
//   - A node with at least 1 in node and at least 1 out node is an internal node
//   - A node with at least 1 in node with no out nodes is a sink
//   - A node with no in nodes and at least 1 out node is a source
//   - A node that has no in nodes and no out nodes (is alone) is a winner if it was source before updating, loser otherwise according to YO-YO with pruning algorithm
func (e *ElectionContext) UpdateStatus() {

	oldStatus := e.status

	if e.InNodesCount() > 0 {
		if e.OutNodesCount() > 0 {
			e.status = InternalNode
		} else {
			e.status = Sink
		}
	} else {
		if e.OutNodesCount() > 0 {
			e.status = Source
		} else {
			// 0 in neighbors, 0 out neighbor
			if oldStatus == Source {
				e.status = Winner
			} else {
				e.status = Loser
			}
		}
	}
}

// updateLinks flips the links after an election round
// Every node that voted no, and to which this node sent no, will have its link flipped
func (e *ElectionContext) updateLinks() {
	var toFlip []node.NodeId

	fmt.Println("Entering update links")

	for _, outNode := range e.OutNodes() {
		vote, err := e.RetrieveVote(outNode)
		if err != nil {
			continue
		}
		if !vote {
			toFlip = append(toFlip, outNode)
		}
	}

	for _, inNode := range e.InNodes() {
		vote, err := e.DetermineVote(inNode)
		if err != nil {
			continue
		}
		if !vote {
			toFlip = append(toFlip, inNode)
		}
	}

	for _, node := range toFlip {
		e.InvertOrientation(node)
	}
}

// DetermineVote calculates the vote that this node should give to proposerId
// If successful, error is nil
func (e *ElectionContext) DetermineVote(proposerId node.NodeId) (bool, error) {
	propose, err := e.RetrieveProposal(proposerId)
	if err != nil {
		return false, err
	}
	return e.currentRound.smallestId == propose, nil
}

// StashFutureProposal saves the proposal ID received from sender sent on the round roundEpoch (greater than the current context round)
// If successful error is nil
func (e *ElectionContext) StashFutureProposal(sender node.NodeId, proposed node.NodeId, roundEpoch uint) error {
	// We can't check for orientation, as the link could get inverted
	_, err := e.RetreiveFutureProposal(sender, roundEpoch)
	if err != nil {
		return err
	}
	round, _ := e.futureRounds[roundEpoch]

	round.receivedProposals[sender] = proposed
	return nil
}

// RetreiveFutureProposal returns the proposal ID received from sender sent on the round roundEpoch  (greater than the current context round)
// If successful error is nil
func (e *ElectionContext) RetreiveFutureProposal(sender node.NodeId, roundEpoch uint) (node.NodeId, error) {
	round, ok := e.futureRounds[roundEpoch]
	if !ok {
		round = NewFutureRoundContext(roundEpoch)
		e.futureRounds[roundEpoch] = round
	}
	propose, ok := round.receivedProposals[sender]
	if !ok {
		return 0, fmt.Errorf("The node %d has not given a future propose yet", sender)
	}
	return propose, nil
}

// StashFutureVote saves the vote received from sender sent on the round roundEpoch (greater than the current context round)
// If successful error is nil
func (e *ElectionContext) StashFutureVote(sender node.NodeId, vote bool, roundEpoch uint) error {
	// We can't check for orientation, as the link could get inverted
	_, err := e.RetreiveFutureVote(sender, roundEpoch)
	if err != nil {
		return err
	}
	round, _ := e.futureRounds[roundEpoch]

	round.receivedVotes[sender] = vote
	return nil
}

// RetreiveFutureVote returns the vote received from sender sent on the round roundEpoch  (greater than the current context round)
// If successful error is nil
func (e *ElectionContext) RetreiveFutureVote(sender node.NodeId, roundEpoch uint) (bool, error) {
	round, ok := e.futureRounds[roundEpoch]
	if !ok {
		round = NewFutureRoundContext(roundEpoch)
		e.futureRounds[roundEpoch] = round
	}
	vote, ok := round.receivedVotes[sender]
	if !ok {
		return false, fmt.Errorf("The node %d has not given a future vote yet", sender)
	}
	return vote, nil
}

// StoreProposal saves the proposal ID received by sender. If successful, error is nil
func (e *ElectionContext) StoreProposal(sender node.NodeId, proposed node.NodeId) error {
	if e.linksOrientation[sender] == Outgoing {
		return fmt.Errorf("Proposal should arrive from incoming links")
	}

	_, err := e.RetrieveProposal(sender)
	if err == nil {
		return fmt.Errorf("Can't store, %d already proposed", sender)
	}
	e.currentRound.awaitedProposals--
	e.currentRound.receivedProposals[sender] = proposed
	if proposed < e.currentRound.smallestId {
		e.currentRound.smallestId = proposed
	}
	return nil
}

// RetrieveProposal returns the proposal ID sent by sender. If successful, error is nil
func (e *ElectionContext) RetrieveProposal(sender node.NodeId) (node.NodeId, error) {
	propose, ok := e.currentRound.receivedProposals[sender]
	if !ok {
		return 0, fmt.Errorf("The node %d has not proposed yet", sender)
	}
	return propose, nil
}

// StoreVote saves the vote received by sender. If successful, error is nil
func (e *ElectionContext) StoreVote(sender node.NodeId, vote bool) error {
	if e.linksOrientation[sender] == Incoming {
		return fmt.Errorf("Votes should arrive from outgoing links")
	}

	_, err := e.RetrieveVote(sender)
	if err == nil {
		return fmt.Errorf("Can't store, %d already voted", sender)
	}

	e.currentRound.awaitedVotes--
	e.currentRound.receivedVotes[sender] = vote
	e.currentRound.forwardedVote = e.currentRound.forwardedVote && vote

	return nil
}

// RetrieveVote returns the vote sent by sender. If successful, error is nil
func (e *ElectionContext) RetrieveVote(sender node.NodeId) (bool, error) {
	vote, ok := e.currentRound.receivedVotes[sender]
	if !ok {
		return false, fmt.Errorf("The node %d has not voted yet", sender)
	}
	return vote, nil
}

// ReceivedAllProposals tells whether all proposals were received
func (e *ElectionContext) ReceivedAllProposals() bool {
	return e.currentRound.awaitedProposals == 0
}

// ReceivedAllVotes tells whether all votes were received
func (e *ElectionContext) ReceivedAllVotes() bool {
	return e.currentRound.awaitedVotes == 0
}

// GetSmallestId retrieves the smallest ID seen
func (e *ElectionContext) GetSmallestId() node.NodeId {
	return e.currentRound.smallestId
}

// GetVoteToForward retrieves the vote store for forwarding
func (e *ElectionContext) GetVoteToForward() bool {
	return e.currentRound.forwardedVote
}

// GetStatus returns the status of the node in this election round
func (e *ElectionContext) GetStatus() ElectionStatus {
	return e.status
}

// SwitchToState switches this node to the given state
func (e *ElectionContext) SwitchToState(s ElectionState) {
	e.state = s
}

// GetState returns the state of the node in this election round
func (e *ElectionContext) GetState() ElectionState {
	return e.state
}

// IsInternal tells wheter the status of the node is source
func (e *ElectionContext) IsSource() bool {
	return e.status == Source
}

// IsInternal tells wheter the status of the node is sink
func (e *ElectionContext) IsSink() bool {
	return e.status == Sink
}

// IsInternal tells wheter the status of the node is internal node
func (e *ElectionContext) IsInternal() bool {
	return e.status == InternalNode
}

// InvertOrientation inverts the orientation of the link {e, n}, where n is the node
// identified by id. If successful, error is nil
func (e *ElectionContext) InvertOrientation(id node.NodeId) error {
	orientation, err := e.GetOrientation(id)
	if err != nil {
		return err
	}
	if err := e.SetOrientation(id, orientation.Inverse()); err != nil {
		return err
	}
	return nil
}

// Nodes returns a slice, inNodes, that has all the nodes whose links are Incoming
func (e *ElectionContext) InNodes() (inNodes []node.NodeId) {
	inNodes, _ = e.Nodes()
	return inNodes
}

// Nodes returns a slice, outNodes, in which all nodes have links that are Outgoing
func (e *ElectionContext) OutNodes() (outNodes []node.NodeId) {
	_, outNodes = e.Nodes()
	return outNodes
}

// OutNodesCount returns the number of incoming links
func (e *ElectionContext) InNodesCount() uint {
	return e.orientationCount[Incoming]
}

// OutNodesCount returns the number of outgoing links
func (e *ElectionContext) OutNodesCount() uint {
	return e.orientationCount[Outgoing]
}

// Nodes returns two slices, inNodes, that has all the nodes whose links are Incoming, and outNodes,
// in which all nodes have links that are Outgoing
func (e *ElectionContext) Nodes() (inNodes []node.NodeId, outNodes []node.NodeId) {
	inNodes = make([]node.NodeId, e.InNodesCount())
	outNodes = make([]node.NodeId, e.OutNodesCount())

	in, out := 0, 0
	for id, orientation := range e.linksOrientation {
		if orientation == Incoming {
			inNodes[in] = id
			in++
		} else {
			outNodes[out] = id
			out++
		}
	}
	return inNodes, outNodes
}
