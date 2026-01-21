package topology

import (
	"fmt"
	"server/cluster/node"
	"sync"
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
	mutex sync.RWMutex

	root          bool
	hasParent     bool
	parent        node.NodeId
	counter       uint64
	state         TreeState
	epoch         uint64
	rootHopCount  uint64
	treeNeighbors map[node.NodeId]bool
	timeouts      map[node.NodeId]uint8
}

func NewTreeManager() *TreeManager {

	return &TreeManager{
		mutex:         sync.RWMutex{},
		root:          false,
		hasParent:     false,
		parent:        0,
		counter:       0,
		epoch:         0,
		rootHopCount:  0,
		state:         TreeIdle,
		treeNeighbors: make(map[node.NodeId]bool),
		timeouts:      make(map[node.NodeId]uint8),
	}
}

func (t *TreeManager) Reset() {
	t.root = false
	t.parent = 0
	t.counter = 0
	t.epoch = 0
	t.state = TreeIdle
	clear(t.treeNeighbors)
	clear(t.timeouts)
}

func (t *TreeManager) GetRootHopCount() uint64 {
	return t.rootHopCount
}

func (t *TreeManager) SetParent(id node.NodeId, rootHopCount uint64) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.IsRoot() {
		return fmt.Errorf("Node is root, can't set parent")
	}
	t.parent = id
	t.rootHopCount = rootHopCount
	t.hasParent = true
	return nil
}
func (t *TreeManager) GetParent() (node.NodeId, bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if t.IsRoot() || !t.hasParent {
		return 0, false
	}
	return t.parent, true
}

func (t *TreeManager) RemoveParent() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.IsRoot() {
		return fmt.Errorf("Node is root")
	}
	t.hasParent = false
	t.parent = 0
	return nil
}

func (t *TreeManager) SetRoot(root bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.root = root

	if root {
		t.rootHopCount = 0
		t.hasParent = false
	}
}

// Returns true if the node is the root of the tree.
func (t *TreeManager) IsRoot() bool {
	return t.root
}

func (t *TreeManager) HasParent() bool {
	return t.hasParent
}

func (t *TreeManager) SetEpoch(epoch uint64) {
	t.epoch = epoch
}

func (t *TreeManager) GetEpoch() uint64 {
	return t.epoch
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
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if _, ok := t.treeNeighbors[id]; ok {
		return fmt.Errorf("Neighbor %d already present", id)
	}
	t.treeNeighbors[id] = true
	return nil
}

func (t *TreeManager) RemoveTreeNeighbor(id node.NodeId) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	delete(t.treeNeighbors, id)
}

func (t *TreeManager) AcknowledgeNo(id node.NodeId) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if _, ok := t.treeNeighbors[id]; ok {
		return fmt.Errorf("Neighbor %d already present", id)
	}
	t.treeNeighbors[id] = false
	return nil
}

func (t *TreeManager) GetTreeNeighbors() (parent node.NodeId, children map[node.NodeId]struct{}, hasParent bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	children = make(map[node.NodeId]struct{})
	if t.root {
		parent = 0
		hasParent = false

		for key, answer := range t.treeNeighbors {
			if answer {
				children[key] = struct{}{}
			}
		}
	} else {
		parent = t.parent
		hasParent = t.hasParent
		for key, answer := range t.treeNeighbors {
			if answer && key != parent {
				children[key] = struct{}{}
			}
		}
	}
	return parent, children, hasParent
}

func (t *TreeManager) GetChildren() map[node.NodeId]struct{} {
	_, children, _ := t.GetTreeNeighbors()
	return children
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
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if _, ok := t.timeouts[treeNeighbor]; !ok {
		t.timeouts[treeNeighbor] = 0
	}
	t.timeouts[treeNeighbor]++
	return t.timeouts[treeNeighbor]
}

func (t *TreeManager) GetTimeout(treeNeighbor node.NodeId) uint8 {
	return t.timeouts[treeNeighbor]
}
