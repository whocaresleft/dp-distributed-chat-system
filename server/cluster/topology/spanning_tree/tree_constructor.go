package spanningtree

import (
	"fmt"
	"server/cluster/node"
	"server/cluster/node/protocol"
)

type TreeConstructor struct {
	root          bool
	parent        node.NodeId
	counter       uint64
	state         protocol.TreeState
	treeNeighbors map[node.NodeId]struct{}
}

func NewTreeConstructor() *TreeConstructor {

	return &TreeConstructor{
		root:          false,
		parent:        0,
		counter:       0,
		state:         protocol.Idle,
		treeNeighbors: make(map[node.NodeId]struct{}),
	}
}

func (t *TreeConstructor) Reset() {
	t.root = false
	t.parent = 0
	t.counter = 0
	t.state = protocol.Idle
	clear(t.treeNeighbors)
}

func (t *TreeConstructor) SetParent(id node.NodeId) {
	t.parent = id
}
func (t *TreeConstructor) GetParent() (node.NodeId, error) {
	if t.IsRoot() {
		return 0, fmt.Errorf("Node is root")
	}
	return t.parent, nil
}

func (t *TreeConstructor) SetRoot(root bool) {
	t.root = root
}
func (t *TreeConstructor) IsRoot() bool {
	return t.root
}

func (t *TreeConstructor) IncreaseCounter() uint64 {
	t.counter++
	return t.counter
}

func (t *TreeConstructor) GetCounter() uint64 {
	return t.counter
}

func (t *TreeConstructor) GetState() protocol.TreeState {
	return t.state
}

func (t *TreeConstructor) SwitchToState(state protocol.TreeState) {
	t.state = state
}

func (t *TreeConstructor) AddTreeNeighbor(id node.NodeId) error {
	if _, ok := t.treeNeighbors[id]; ok {
		return fmt.Errorf("Neighbor already present")
	}
	t.treeNeighbors[id] = struct{}{}
	return nil
}

func (t *TreeConstructor) GetTreeNeighbors() []node.NodeId {
	neighbors := make([]node.NodeId, len(t.treeNeighbors))

	i := 0
	for key := range t.treeNeighbors {
		neighbors[i] = key
		i++
	}

	return neighbors
}
