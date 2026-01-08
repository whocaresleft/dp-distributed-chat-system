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

type roundContext struct {
	epoch uint

	smallestId    node.NodeId
	forwardedVote bool

	// YO- Information
	awaitedProposals  uint
	receivedProposals map[node.NodeId]node.NodeId

	// -YO Information
	awaitedVotes  uint
	receivedVotes map[node.NodeId]bool

	pruneList []node.NodeId
}

type futureRoundContext struct {
	epoch             uint
	receivedVotes     map[node.NodeId]bool
	receivedProposals map[node.NodeId]node.NodeId
}

func NewFutureRoundContext(epoch uint) *futureRoundContext {
	return &futureRoundContext{
		epoch:             epoch,
		receivedProposals: make(map[node.NodeId]node.NodeId),
		receivedVotes:     make(map[node.NodeId]bool),
	}
}

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

func (r *roundContext) setupNewRound(epoch uint) {
	r.epoch = epoch
	r.smallestId = node.NodeId(^uint64(0))
	r.forwardedVote = true
	r.awaitedProposals = 0
	clear(r.receivedProposals)
	r.awaitedVotes = 0
	clear(r.receivedVotes)
}

type ElectionContext struct {
	receivedStart  bool
	receivedLeader bool

	status           ElectionStatus
	state            ElectionState
	currentRound     *roundContext
	futureRounds     map[uint]*futureRoundContext
	linksOrientation map[node.NodeId]LinkDirection
	orientationCount map[LinkDirection]uint
}

func NewElectionContext() *ElectionContext {
	orientationCount := make(map[LinkDirection]uint)
	orientationCount[Incoming] = 0
	orientationCount[Outgoing] = 0
	return &ElectionContext{
		false,
		false,
		Source,
		Idle,
		NewRoundContext(0),
		make(map[uint]*futureRoundContext),
		make(map[node.NodeId]LinkDirection),
		orientationCount,
	}
}

func (e *ElectionContext) Reset() {
	e.receivedStart = false
	e.receivedLeader = false
	e.status = Source
	e.state = Idle
	e.orientationCount[Incoming] = 0
	e.orientationCount[Outgoing] = 0
	clear(e.linksOrientation)
	e.FirstRound()
}

func (e *ElectionContext) GetAllProposals() map[node.NodeId]node.NodeId {
	return e.currentRound.receivedProposals
}

func (e *ElectionContext) PruneThisRound(id node.NodeId) {
	e.currentRound.pruneList = append(e.currentRound.pruneList, id)
}

func (e *ElectionContext) GetAllVotes() map[node.NodeId]bool {
	return e.currentRound.receivedVotes
}

func (e *ElectionContext) FirstRound() {
	e.currentRound.setupNewRound(0)
}

func (e *ElectionContext) prune() {
	for _, id := range e.currentRound.pruneList {
		e.Remove(id)
	}
}

func (e *ElectionContext) NextRound() {
	e.updateLinks()
	e.prune()
	e.updateStatus()

	nextRound := e.CurrentRound() + 1
	e.currentRound.setupNewRound(nextRound)

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

func (e *ElectionContext) HasReceivedStart() bool {
	return e.receivedStart
}

func (e *ElectionContext) HasReceivedLeader() bool {
	return e.receivedLeader
}

func (e *ElectionContext) SetStartReceived() {
	e.receivedStart = true
}

func (e *ElectionContext) SetLeaderReceived() {
	e.receivedLeader = true
}

func (e *ElectionContext) CurrentRound() uint {
	return e.currentRound.epoch
}

func (e *ElectionContext) Exists(id node.NodeId) bool {
	_, ok := e.linksOrientation[id]
	return ok
}

func (e *ElectionContext) Add(id node.NodeId, orientation LinkDirection) error {
	if e.Exists(id) {
		return fmt.Errorf("Node %d is already present", id)
	}
	e.linksOrientation[id] = orientation
	e.orientationCount[orientation]++
	return nil
}

func (e *ElectionContext) Remove(id node.NodeId) error {
	if !e.Exists(id) {
		return fmt.Errorf("Node %d wasn't present", id)
	}
	orientation := e.linksOrientation[id]
	delete(e.linksOrientation, id)
	e.orientationCount[orientation]--
	return nil
}

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

func (e *ElectionContext) GetOrientation(id node.NodeId) (LinkDirection, error) {
	if !e.Exists(id) {
		return false, fmt.Errorf("Node %d is not present, you can add it with .Add()", id)
	}
	return e.linksOrientation[id], nil
}

func (e *ElectionContext) updateStatus() {

	oldStatus := e.status

	if e.orientationCount[Incoming] > 0 {
		if e.orientationCount[Outgoing] > 0 {
			e.status = InternalNode
		} else {
			e.status = Sink
		}
	} else {
		if e.orientationCount[Outgoing] > 0 {
			e.status = Source
		} else {
			// 0 in neighbors, 0 out neighbor
			if oldStatus == Source {
				e.status = Leader
			} else {
				e.status = Lost
			}
		}
	}
}

func (e *ElectionContext) updateLinks() {

	var toFlip []node.NodeId

	for _, outNode := range e.OutNodes() {
		vote, err := e.DetermineVote(outNode)
		if err != nil {
			continue
		}
		if !vote {
			toFlip = append(toFlip, outNode)
		}
	}

	for _, inNode := range e.InNodes() {
		vote, err := e.RetrieveVote(inNode)
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

func (e *ElectionContext) DetermineVote(proposerId node.NodeId) (bool, error) {
	propose, err := e.RetrieveProposal(proposerId)
	if err != nil {
		return false, err
	}
	return e.currentRound.smallestId == propose, nil
}

func (e *ElectionContext) StoreProposal(sender node.NodeId, proposed node.NodeId) error {
	if e.linksOrientation[sender] == Outgoing {
		return fmt.Errorf("Proposal should arrive from incoming links")
	}

	_, err := e.RetrieveProposal(sender)
	if err != nil {
		return err
	}
	e.currentRound.awaitedProposals--
	e.currentRound.receivedProposals[sender] = proposed
	if proposed < e.currentRound.smallestId {
		e.currentRound.smallestId = proposed
	}
	return nil
}

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

func (e *ElectionContext) RetrieveProposal(sender node.NodeId) (node.NodeId, error) {
	propose, ok := e.currentRound.receivedProposals[sender]
	if !ok {
		return 0, fmt.Errorf("The node %d has not proposed yet", sender)
	}
	return propose, nil
}

func (e *ElectionContext) StoreVote(sender node.NodeId, vote bool) error {
	if e.linksOrientation[sender] == Incoming {
		return fmt.Errorf("Votes should arrive from outgoing links")
	}

	_, err := e.RetrieveVote(sender)
	if err != nil {
		return err
	}

	e.currentRound.awaitedVotes--
	e.currentRound.receivedVotes[sender] = vote
	e.currentRound.forwardedVote = e.currentRound.forwardedVote && vote
	return nil
}

func (e *ElectionContext) RetrieveVote(sender node.NodeId) (bool, error) {
	vote, ok := e.currentRound.receivedVotes[sender]
	if !ok {
		return false, fmt.Errorf("The node %d has not voted yet", sender)
	}
	return vote, nil
}

func (e *ElectionContext) ReceivedAllProposals() bool {
	return e.currentRound.awaitedProposals == 0
}

func (e *ElectionContext) ReceivecAllVotes() bool {
	return e.currentRound.awaitedVotes == 0
}

func (e *ElectionContext) GetSmallestId() node.NodeId {
	return e.currentRound.smallestId
}

func (e *ElectionContext) GetVoteToForward() bool {
	return e.currentRound.forwardedVote
}

func (e *ElectionContext) GetStatus() ElectionStatus {
	return e.status
}

func (e *ElectionContext) SwitchToState(s ElectionState) {
	e.state = s
}

func (e *ElectionContext) GetState() ElectionState {
	return e.state
}

func (e *ElectionContext) IsSource() bool {
	return e.status == Source
}

func (e *ElectionContext) IsSink() bool {
	return e.status == Sink
}

func (e *ElectionContext) IsInternal() bool {
	return e.status == InternalNode
}

func (e *ElectionContext) InvertOrientation(id node.NodeId) error {
	orientation, err := e.GetOrientation(id)
	if err != nil {
		return err
	}
	if err := e.SetOrientation(id, orientation.inverse()); err != nil {
		return err
	}
	return nil
}

func (e *ElectionContext) InNodes() []node.NodeId {
	in, _ := e.Nodes()
	return in
}

func (e *ElectionContext) OutNodes() []node.NodeId {
	_, out := e.Nodes()
	return out
}

func (e *ElectionContext) InNodesCount() uint {
	return e.orientationCount[Incoming]
}

func (e *ElectionContext) OutNodesCount() uint {
	return e.orientationCount[Outgoing]
}

func (e *ElectionContext) Nodes() ([]node.NodeId, []node.NodeId) {
	inNodes := make([]node.NodeId, e.InNodesCount())
	outNodes := make([]node.NodeId, e.OutNodesCount())

	for id, orientation := range e.linksOrientation {
		if orientation == Incoming {
			inNodes = append(inNodes, id)
		} else {
			outNodes = append(outNodes, id)
		}
	}
	return inNodes, outNodes
}
