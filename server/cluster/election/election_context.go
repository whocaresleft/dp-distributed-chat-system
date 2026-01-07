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

type ElectionContext struct {
	status           ElectionStatus
	state            ElectionState
	round            uint
	done             bool
	linksOrientation map[node.NodeId]LinkDirection
	orientationCount map[LinkDirection]uint
}

func NewElectionContext() *ElectionContext {
	orientationCount := make(map[LinkDirection]uint)
	orientationCount[Incoming] = 0
	orientationCount[Outgoing] = 0
	return &ElectionContext{Source, Idle, 0, false, make(map[node.NodeId]LinkDirection), orientationCount}
}

func (e *ElectionContext) GetRound() uint {
	return e.round
}

func (e *ElectionContext) IncreaseRound() {
	e.round++
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

func (e *ElectionContext) UpdateStatus() {

	if e.orientationCount[Incoming] > 0 {
		if e.orientationCount[Outgoing] > 0 {
			e.status = InternalNode
		} else {
			e.status = Sink
		}
	} else {
		e.status = Source
	}
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

func (e *ElectionContext) IsDone() bool {
	return e.done
}

func (e *ElectionContext) SetDone() {
	e.done = true
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
