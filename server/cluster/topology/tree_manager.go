package topology

import (
	"fmt"
	"server/cluster/node"
)

type TreeState uint8

const (
	TreeIdle TreeState = iota
	TreeActive
	TreeDone
)

var treeStatesNames = map[TreeState]string{
	TreeIdle:   "Idle",
	TreeActive: "Active",
	TreeDone:   "Done",
}

func (t TreeState) String() string {
	return treeStatesNames[t]
}

const TolerableTreeNeighborTimeouts = uint8(11)

// A Tree Manager is a component of a node that handles the links between nodes inside the Shortest Path Tree created after the leader's election.
type TreeManager struct {
	root          bool
	parent        node.NodeId
	counter       uint64
	state         TreeState
	treeNeighbors map[node.NodeId]bool
	timeouts      map[node.NodeId]uint8
}

func NewTreeManager() *TreeManager {

	return &TreeManager{
		root:          false,
		parent:        0,
		counter:       0,
		state:         TreeIdle,
		treeNeighbors: make(map[node.NodeId]bool),
		timeouts:      make(map[node.NodeId]uint8),
	}
}

func (t *TreeManager) Reset() {
	t.root = false
	t.parent = 0
	t.counter = 0
	t.state = TreeIdle
	clear(t.treeNeighbors)
	clear(t.timeouts)
}

func (t *TreeManager) SetParent(id node.NodeId) {
	t.parent = id
}
func (t *TreeManager) GetParent() (node.NodeId, error) {
	if t.IsRoot() {
		return 0, fmt.Errorf("Node is root")
	}
	return t.parent, nil
}

func (t *TreeManager) SetRoot(root bool) {
	t.root = root
}

// Returns true if the node is the root of the tree.
func (t *TreeManager) IsRoot() bool {
	return t.root
}

func (t *TreeManager) GetCounter() int {
	return len(t.treeNeighbors)
}

func (t *TreeManager) GetState() TreeState {
	return t.state
}

func (t *TreeManager) SwitchToState(state TreeState) {
	t.state = state
}

func (t *TreeManager) AddTreeNeighbor(id node.NodeId) error {
	if _, ok := t.treeNeighbors[id]; ok {
		return fmt.Errorf("Neighbor %d already present", id)
	}
	t.treeNeighbors[id] = true
	return nil
}

func (t *TreeManager) RemoveTreeNeighbor(id node.NodeId) error {
	if _, ok := t.treeNeighbors[id]; !ok {
		return fmt.Errorf("Neighbor %d was not present", id)
	}
	delete(t.treeNeighbors, id)
	return nil
}

func (t *TreeManager) AcknowledgeNo(id node.NodeId) {
	t.treeNeighbors[id] = false
}

func (t *TreeManager) GetTreeNeighbors() (parent node.NodeId, children []node.NodeId, hasParent bool) {

	children = make([]node.NodeId, 0)
	if t.IsRoot() {
		parent = 0
		hasParent = false

		for key, answer := range t.treeNeighbors {
			if answer {
				children = append(children, key)
			}
		}
	} else {
		parent = t.parent
		hasParent = true
		for key, answer := range t.treeNeighbors {
			if answer && key != parent {
				children = append(children, key)
			}
		}
	}
	return parent, children, hasParent
}

func (t *TreeManager) HasAnswered(id node.NodeId) bool {
	_, ok := t.treeNeighbors[id]
	return ok
}

// Returns true when there are no children.
func (t *TreeManager) IsLeaf() bool {
	_, children, _ := t.GetTreeNeighbors()
	return len(children) == 0
}

func (t *TreeManager) IncreaseTimeout(treeNeighbor node.NodeId) uint8 {
	if _, ok := t.timeouts[treeNeighbor]; !ok {
		t.timeouts[treeNeighbor] = 0
	}
	t.timeouts[treeNeighbor]++
	return t.timeouts[treeNeighbor]
}

func (t *TreeManager) GetTimeout(treeNeighbor node.NodeId) uint8 {
	return t.timeouts[treeNeighbor]
}
